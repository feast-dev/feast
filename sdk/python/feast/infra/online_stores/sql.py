# Copyright 2021 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import itertools
import sqlalchemy
from datetime import datetime
import traceback
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from pydantic import StrictStr
from pydantic.schema import Literal

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.infra_object import SQL_INFRA_OBJECT_CLASS_TYPE, InfraObject
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.core.InfraObject_pb2 import InfraObject as InfraObjectProto
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.core.SqliteTable_pb2 import SqliteTable as SqlTableProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.usage import log_exceptions_and_usage, tracing_span
from feast.utils import to_naive_utc


def createFeatureSqlTable(table_name: str, metadata: sqlalchemy.MetaData):
    return sqlalchemy.Table(
        table_name,
        metadata,
        sqlalchemy.Column("entity_key", sqlalchemy.LargeBinary, primary_key=True),
        sqlalchemy.Column("feature_name", sqlalchemy.String(255), primary_key=True),
        sqlalchemy.Column("value", sqlalchemy.LargeBinary),
        sqlalchemy.Column("event_ts", sqlalchemy.DateTime),
        sqlalchemy.Column("created_ts", sqlalchemy.DateTime)
    )


class SqlOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for local (SQL-based) store"""

    type: Literal[
        "sql", "feast.infra.online_stores.sql.SqlOnlineStore"
    ] = "sql"
    """ Online store type selector"""

    path: StrictStr = "dialect+driver://username:password@host:port/database"
    """ Path to sql db """


class SqlOnlineStore(OnlineStore):
    """
    SQL implementation of the online store interface, genericised by SQLAlchemy.
    """

    @staticmethod
    def _get_db_path(config: RepoConfig) -> str:
        assert (
            config.online_store.type == "sql"
            or config.online_store.type.endswith("SqlOnlineStore")
        )

        return config.online_store.path

    def _get_engine(self, config: RepoConfig):
        return sqlalchemy.create_engine(
            self._get_db_path(config)
        )

    def _get_conn(self, config: RepoConfig):
        return self._get_engine(config).begin()

    @log_exceptions_and_usage(online_store="sql")
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:

        project = config.project
        table = createFeatureSqlTable(
            _table_id(project, table),
            sqlalchemy.MetaData()
        )

        engine = self._get_engine(config)
        with engine.connect() as conn:
            for entity_key, values, timestamp, created_ts in data:
                entity_key_bin = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
                timestamp = to_naive_utc(timestamp)
                if created_ts is not None:
                    created_ts = to_naive_utc(created_ts)

                for feature_name, val in values.items():
                    try:
                        with conn.begin():
                            conn.execute(
                                sqlalchemy.insert(
                                    table
                                ).values(
                                    entity_key=entity_key_bin,
                                    feature_name=feature_name,
                                    value=val.SerializeToString(),
                                    event_ts=timestamp,
                                    created_ts=created_ts,
                                )
                            )
                    except:
                        with conn.begin():
                            conn.execute(
                                sqlalchemy.update(
                                    table
                                ).where(
                                    table.c.entity_key == entity_key_bin,
                                    table.c.feature_name == feature_name
                                ).values(
                                    value=val.SerializeToString(),
                                    event_ts=timestamp,
                                    created_ts=created_ts,
                                )
                            )
                if progress:
                    progress(1)

    @log_exceptions_and_usage(online_store="sql")
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        table = createFeatureSqlTable(
            _table_id(config.project, table),
            sqlalchemy.MetaData()
        )
        with tracing_span(name="remote_call"):
            # Fetch all entities in one go
            with self._get_conn(config) as conn:
                rows = conn.execute(
                    sqlalchemy.select(
                        table.c.entity_key,
                        table.c.feature_name,
                        table.c.value,
                        table.c.event_ts
                    ).where(
                        table.c.entity_key.in_(
                            [
                                serialize_entity_key(
                                    entity_key,
                                    entity_key_serialization_version=config.entity_key_serialization_version,
                                )
                                for entity_key in entity_keys
                            ]
                        )
                    ).order_by(
                        table.c.entity_key.asc()
                    )
                ).fetchall()

        rows = {
            k: list(group) for k, group in itertools.groupby(rows, key=lambda r: r[0])
        }
        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            res = {}
            res_ts = None
            for _, feature_name, val_bin, ts in rows.get(entity_key_bin, []):
                val = ValueProto()
                val.ParseFromString(val_bin)
                res[feature_name] = val
                res_ts = ts

            if not res:
                result.append((None, None))
            else:
                result.append((res_ts, res))
        return result

    @log_exceptions_and_usage(online_store="sql")
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        project = config.project
        metadata = sqlalchemy.MetaData()
        engine = self._get_engine(config)
        # build all the tables to keep in the metadata instance
        for table in tables_to_keep:
            createFeatureSqlTable(
                _table_id(project, table),
                metadata
            )
        metadata.create_all(engine)

        with engine.begin() as conn:
            for table in tables_to_keep:
                # conn.execute(
                #     sqlalchemy.text(f"CREATE TABLE IF NOT EXISTS {_table_id(project, table)} (entity_key LargeBinary, feature_name TEXT, value LargeBinary, event_ts DateTime, created_ts DateTime,  PRIMARY KEY(entity_key, feature_name))")
                # )
                conn.execute(
                    sqlalchemy.text(f"CREATE INDEX IF NOT EXISTS {_table_id(project, table)}_ek ON {_table_id(project, table)} (entity_key);")
                )

            for table in tables_to_delete:
                conn.execute(
                    sqlalchemy.text(f"DROP TABLE IF EXISTS {_table_id(project, table)}")
                )

    @log_exceptions_and_usage(online_store="sql")
    def plan(
        self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> List[InfraObject]:
        project = config.project

        infra_objects: List[InfraObject] = [
            SqlTable(
                path=self._get_db_path(config),
                name=_table_id(project, FeatureView.from_proto(view)),
            )
            for view in [
                *desired_registry_proto.feature_views,
                *desired_registry_proto.stream_feature_views,
            ]
        ]
        return infra_objects

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        self.update(
            config,
            tables_to_delete=tables,
            tables_to_keep=[],
            entities_to_delete=entities,
            entities_to_keep=[],
            partial=False,
        )


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


class SqlTable(InfraObject):
    """
    A Sql table managed by Feast.

    Attributes:
        path: The absolute path of the Sql file.
        name: The name of the table.
        conn: SQL connection.
    """

    path: str
    engine: sqlalchemy.engine.Engine
    metadata: sqlalchemy.MetaData

    def __init__(self, path: str, name: str):
        super().__init__(name)
        self.path = path
        self.engine = sqlalchemy.create_engine(path)
        self.metadata = sqlalchemy.MetaData()
        createFeatureSqlTable(self.name, self.metadata)

    def to_infra_object_proto(self) -> InfraObjectProto:
        sql_table_proto = self.to_proto()
        return InfraObjectProto(
            infra_object_class_type=SQL_INFRA_OBJECT_CLASS_TYPE,
            sqlite_table=sql_table_proto,
        )

    def to_proto(self) -> Any:
        sql_table_proto = SqlTableProto()
        sql_table_proto.path = self.path
        sql_table_proto.name = self.name
        return sql_table_proto

    @staticmethod
    def from_infra_object_proto(infra_object_proto: InfraObjectProto) -> Any:
        return SqlTable(
            path=infra_object_proto.sql_table.path,
            name=infra_object_proto.sql_table.name,
        )

    @staticmethod
    def from_proto(sql_table_proto: SqlTableProto) -> Any:
        return SqlTable(
            path=sql_table_proto.path,
            name=sql_table_proto.name,
        )

    def update(self):
        self.metadata.create_all(self.engine)
        with self.engine.begin() as conn:
            # conn.execute(
            #     sqlalchemy.text(f"CREATE TABLE IF NOT EXISTS {self.name} (entity_key LargeBinary, feature_name TEXT, value LargeBinary, event_ts DateTime, created_ts DateTime,  PRIMARY KEY(entity_key, feature_name))")
            # )

            conn.execute(
                sqlalchemy.text(f"CREATE INDEX IF NOT EXISTS {self.name}_ek ON {self.name} (entity_key);")
            )

    def teardown(self):
        with self.engine.begin() as conn:
            conn.execute(
                sqlalchemy.text(f"DROP TABLE IF EXISTS {self.name}")
            )
