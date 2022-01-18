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
import os
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from pydantic import StrictStr
from pydantic.schema import Literal

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.infra_object import SQLITE_INFRA_OBJECT_CLASS_TYPE, InfraObject
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.core.InfraObject_pb2 import InfraObject as InfraObjectProto
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.core.SqliteTable_pb2 import SqliteTable as SqliteTableProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.usage import log_exceptions_and_usage, tracing_span
from feast.utils import to_naive_utc


class SqliteOnlineStoreConfig(FeastConfigBaseModel):
    """ Online store config for local (SQLite-based) store """

    type: Literal[
        "sqlite", "feast.infra.online_stores.sqlite.SqliteOnlineStore"
    ] = "sqlite"
    """ Online store type selector"""

    path: StrictStr = "data/online.db"
    """ (optional) Path to sqlite db """


class SqliteOnlineStore(OnlineStore):
    """
    OnlineStore is an object used for all interaction between Feast and the service used for offline storage of
    features.

    Attributes:
        _conn: SQLite connection.
    """

    _conn: Optional[sqlite3.Connection] = None

    @staticmethod
    def _get_db_path(config: RepoConfig) -> str:
        assert (
            config.online_store.type == "sqlite"
            or config.online_store.type.endswith("SqliteOnlineStore")
        )

        if config.repo_path and not Path(config.online_store.path).is_absolute():
            db_path = str(config.repo_path / config.online_store.path)
        else:
            db_path = config.online_store.path
        return db_path

    def _get_conn(self, config: RepoConfig):
        if not self._conn:
            db_path = self._get_db_path(config)
            self._conn = _initialize_conn(db_path)
        return self._conn

    @log_exceptions_and_usage(online_store="sqlite")
    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:

        conn = self._get_conn(config)

        project = config.project

        with conn:
            for entity_key, values, timestamp, created_ts in data:
                entity_key_bin = serialize_entity_key(entity_key)
                timestamp = to_naive_utc(timestamp)
                if created_ts is not None:
                    created_ts = to_naive_utc(created_ts)

                for feature_name, val in values.items():
                    conn.execute(
                        f"""
                            UPDATE {_table_id(project, table)}
                            SET value = ?, event_ts = ?, created_ts = ?
                            WHERE (entity_key = ? AND feature_name = ?)
                        """,
                        (
                            # SET
                            val.SerializeToString(),
                            timestamp,
                            created_ts,
                            # WHERE
                            entity_key_bin,
                            feature_name,
                        ),
                    )

                    conn.execute(
                        f"""INSERT OR IGNORE INTO {_table_id(project, table)}
                            (entity_key, feature_name, value, event_ts, created_ts)
                            VALUES (?, ?, ?, ?, ?)""",
                        (
                            entity_key_bin,
                            feature_name,
                            val.SerializeToString(),
                            timestamp,
                            created_ts,
                        ),
                    )
                if progress:
                    progress(1)

    @log_exceptions_and_usage(online_store="sqlite")
    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        conn = self._get_conn(config)
        cur = conn.cursor()

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []

        with tracing_span(name="remote_call"):
            # Fetch all entities in one go
            cur.execute(
                f"SELECT entity_key, feature_name, value, event_ts "
                f"FROM {_table_id(config.project, table)} "
                f"WHERE entity_key IN ({','.join('?' * len(entity_keys))}) "
                f"ORDER BY entity_key",
                [serialize_entity_key(entity_key) for entity_key in entity_keys],
            )
            rows = cur.fetchall()

        rows = {
            k: list(group) for k, group in itertools.groupby(rows, key=lambda r: r[0])
        }
        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(entity_key)
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

    @log_exceptions_and_usage(online_store="sqlite")
    def update(
        self,
        config: RepoConfig,
        tables_to_delete: Sequence[FeatureView],
        tables_to_keep: Sequence[FeatureView],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
        partial: bool,
    ):
        conn = self._get_conn(config)
        project = config.project

        for table in tables_to_keep:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {_table_id(project, table)} (entity_key BLOB, feature_name TEXT, value BLOB, event_ts timestamp, created_ts timestamp,  PRIMARY KEY(entity_key, feature_name))"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {_table_id(project, table)}_ek ON {_table_id(project, table)} (entity_key);"
            )

        for table in tables_to_delete:
            conn.execute(f"DROP TABLE IF EXISTS {_table_id(project, table)}")

    @log_exceptions_and_usage(online_store="sqlite")
    def plan(
        self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> List[InfraObject]:
        project = config.project

        infra_objects: List[InfraObject] = [
            SqliteTable(
                path=self._get_db_path(config),
                name=_table_id(project, FeatureView.from_proto(view)),
            )
            for view in desired_registry_proto.feature_views
        ]
        return infra_objects

    def teardown(
        self,
        config: RepoConfig,
        tables: Sequence[FeatureView],
        entities: Sequence[Entity],
    ):
        try:
            os.unlink(self._get_db_path(config))
        except FileNotFoundError:
            pass


def _initialize_conn(db_path: str):
    Path(db_path).parent.mkdir(exist_ok=True)
    return sqlite3.connect(
        db_path, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


class SqliteTable(InfraObject):
    """
    A Sqlite table managed by Feast.

    Attributes:
        path: The absolute path of the Sqlite file.
        name: The name of the table.
        conn: SQLite connection.
    """

    path: str
    name: str
    conn: sqlite3.Connection

    def __init__(self, path: str, name: str):
        self.path = path
        self.name = name
        self.conn = _initialize_conn(path)

    def to_infra_object_proto(self) -> InfraObjectProto:
        sqlite_table_proto = self.to_proto()
        return InfraObjectProto(
            infra_object_class_type=SQLITE_INFRA_OBJECT_CLASS_TYPE,
            sqlite_table=sqlite_table_proto,
        )

    def to_proto(self) -> Any:
        sqlite_table_proto = SqliteTableProto()
        sqlite_table_proto.path = self.path
        sqlite_table_proto.name = self.name
        return sqlite_table_proto

    @staticmethod
    def from_infra_object_proto(infra_object_proto: InfraObjectProto) -> Any:
        return SqliteTable(
            path=infra_object_proto.sqlite_table.path,
            name=infra_object_proto.sqlite_table.name,
        )

    @staticmethod
    def from_proto(sqlite_table_proto: SqliteTableProto) -> Any:
        return SqliteTable(path=sqlite_table_proto.path, name=sqlite_table_proto.name,)

    def update(self):
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.name} (entity_key BLOB, feature_name TEXT, value BLOB, event_ts timestamp, created_ts timestamp,  PRIMARY KEY(entity_key, feature_name))"
        )
        self.conn.execute(
            f"CREATE INDEX IF NOT EXISTS {self.name}_ek ON {self.name} (entity_key);"
        )

    def teardown(self):
        self.conn.execute(f"DROP TABLE IF EXISTS {self.name}")
