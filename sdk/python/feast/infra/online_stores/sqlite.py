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
import logging
import os
import sqlite3
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence, Tuple

from pydantic import StrictStr

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.infra_object import SQLITE_INFRA_OBJECT_CLASS_TYPE, InfraObject
from feast.infra.key_encoding_utils import (
    deserialize_entity_key,
    serialize_entity_key,
    serialize_f32,
)
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.protos.feast.core.InfraObject_pb2 import InfraObject as InfraObjectProto
from feast.protos.feast.core.Registry_pb2 import Registry as RegistryProto
from feast.protos.feast.core.SqliteTable_pb2 import SqliteTable as SqliteTableProto
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.utils import _build_retrieve_online_document_record, to_naive_utc


def adapt_date_iso(val: date):
    """Adapt datetime.date to ISO 8601 date."""
    return val.isoformat()


def adapt_datetime_iso(val: datetime):
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return val.isoformat()


def adapt_datetime_epoch(val: datetime):
    """Adapt datetime.datetime to Unix timestamp."""
    return int(val.timestamp())


sqlite3.register_adapter(date, adapt_date_iso)
sqlite3.register_adapter(datetime, adapt_datetime_iso)
sqlite3.register_adapter(datetime, adapt_datetime_epoch)


def convert_date(val: bytes):
    """Convert ISO 8601 date to datetime.date object."""
    return date.fromisoformat(val.decode())


def convert_datetime(val: bytes):
    """Convert ISO 8601 datetime to datetime.datetime object."""
    return datetime.fromisoformat(val.decode())


def convert_timestamp(val: bytes):
    """Convert Unix epoch timestamp to datetime.datetime object."""
    return datetime.fromtimestamp(int(val))


sqlite3.register_converter("date", convert_date)
sqlite3.register_converter("datetime", convert_datetime)
sqlite3.register_converter("timestamp", convert_timestamp)


class SqliteOnlineStoreConfig(FeastConfigBaseModel, VectorStoreConfig):
    """Online store config for local (SQLite-based) store"""

    type: Literal["sqlite", "feast.infra.online_stores.sqlite.SqliteOnlineStore"] = (
        "sqlite"
    )
    """ Online store type selector"""

    path: StrictStr = "data/online.db"
    """ (optional) Path to sqlite db """

    vector_enabled: bool = False
    vector_len: Optional[int] = None


class SqliteOnlineStore(OnlineStore):
    """
    SQLite implementation of the online store interface. Not recommended for production usage.

    Attributes:
        _conn: SQLite connection.
    """

    _conn: Optional[sqlite3.Connection] = None

    @staticmethod
    def _get_db_path(config: RepoConfig) -> str:
        online_store = config.online_store
        if not isinstance(online_store, SqliteOnlineStoreConfig):
            raise ValueError("online_store must be SqliteOnlineStoreConfig")
        if config.repo_path and not Path(online_store.path).is_absolute():
            return str(config.repo_path / online_store.path)
        return str(online_store.path)

    def _get_conn(self, config: RepoConfig):
        if not self._conn:
            db_path = self._get_db_path(config)
            self._conn = _initialize_conn(db_path)
            online_store = config.online_store
            if not isinstance(online_store, SqliteOnlineStoreConfig):
                raise ValueError("online_store must be SqliteOnlineStoreConfig")
            if sys.version_info[0:2] == (3, 10) and online_store.vector_enabled:
                import sqlite_vec  # noqa: F401

                self._conn.enable_load_extension(True)  # type: ignore
                sqlite_vec.load(self._conn)

        return self._conn

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[
                EntityKeyProto,
                Dict[str, ValueProto],
                datetime,
                Optional[datetime],
            ]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        online_store = config.online_store
        if not isinstance(online_store, SqliteOnlineStoreConfig):
            raise ValueError("online_store must be SqliteOnlineStoreConfig")
        conn = self._get_conn(config)

        project = config.project

        with conn:
            for entity_key, values, timestamp, created_ts in data:
                entity_key_bin = serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
                timestamp = to_naive_utc(timestamp)
                if created_ts is not None:
                    created_ts = to_naive_utc(created_ts)

                table_name = _table_id(project, table)
                for feature_name, val in values.items():
                    online_store = config.online_store
                    if not isinstance(online_store, SqliteOnlineStoreConfig):
                        raise ValueError("online_store must be SqliteOnlineStoreConfig")
                    if online_store.vector_enabled and online_store.vector_len:
                        vector_bin = serialize_f32(
                            val.float_list_val.val,
                            online_store.vector_len,
                        )  # type: ignore
                        conn.execute(
                            f"""
                                    UPDATE {table_name}
                                    SET value = ?, vector_value = ?, event_ts = ?, created_ts = ?
                                    WHERE (entity_key = ? AND feature_name = ?)
                                """,
                            (
                                # SET
                                val.SerializeToString(),
                                vector_bin,
                                timestamp,
                                created_ts,
                                # WHERE
                                entity_key_bin,
                                feature_name,
                            ),
                        )

                        conn.execute(
                            f"""INSERT OR IGNORE INTO {table_name}
                                (entity_key, feature_name, value, vector_value, event_ts, created_ts)
                                VALUES (?, ?, ?, ?, ?, ?)""",
                            (
                                entity_key_bin,
                                feature_name,
                                val.SerializeToString(),
                                vector_bin,
                                timestamp,
                                created_ts,
                            ),
                        )

                    else:
                        conn.execute(
                            f"""
                                UPDATE {table_name}
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
                            f"""INSERT OR IGNORE INTO {table_name}
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

        # Fetch all entities in one go
        cur.execute(
            f"SELECT entity_key, feature_name, value, event_ts "
            f"FROM {_table_id(config.project, table)} "
            f"WHERE entity_key IN ({','.join('?' * len(entity_keys))}) "
            f"ORDER BY entity_key",
            [
                serialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
                for entity_key in entity_keys
            ],
        )
        rows = cur.fetchall()

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
                f"CREATE TABLE IF NOT EXISTS {_table_id(project, table)} (entity_key BLOB, feature_name TEXT, value BLOB, vector_value BLOB, event_ts timestamp, created_ts timestamp,  PRIMARY KEY(entity_key, feature_name))"
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {_table_id(project, table)}_ek ON {_table_id(project, table)} (entity_key);"
            )

        for table in tables_to_delete:
            conn.execute(f"DROP TABLE IF EXISTS {_table_id(project, table)}")

    def plan(
        self, config: RepoConfig, desired_registry_proto: RegistryProto
    ) -> List[InfraObject]:
        project = config.project

        infra_objects: List[InfraObject] = [
            SqliteTable(
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
        try:
            os.unlink(self._get_db_path(config))
        except FileNotFoundError:
            pass

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_feature: Optional[str],
        requested_featuers: Optional[List[str]],
        embedding: List[float],
        top_k: int,
        distance_metric: Optional[str] = None,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[ValueProto],
            Optional[ValueProto],
            Optional[ValueProto],
        ]
    ]:
        """

        Args:
            config: Feast configuration object
            table: FeatureView object as the table to search
            requested_feature: The requested feature as the column to search
            embedding: The query embedding to search for
            top_k: The number of items to return
        Returns:
            List of tuples containing the event timestamp, the document feature, the vector value, and the distance
        """
        online_store = config.online_store
        if not isinstance(online_store, SqliteOnlineStoreConfig):
            raise ValueError("online_store must be SqliteOnlineStoreConfig")
        if not online_store.vector_enabled:
            raise ValueError("sqlite-vss is not enabled in the online store config")

        conn = self._get_conn(config)
        cur = conn.cursor()

        # Convert the embedding to a binary format instead of using SerializeToString()
        online_store = config.online_store
        if not isinstance(online_store, SqliteOnlineStoreConfig):
            raise ValueError("online_store must be SqliteOnlineStoreConfig")
        if not online_store.vector_len:
            raise ValueError("vector_len is not configured in the online store config")
        query_embedding_bin = serialize_f32(embedding, online_store.vector_len)  # type: ignore
        table_name = _table_id(config.project, table)

        cur.execute(
            f"""
            CREATE VIRTUAL TABLE vec_example using vec0(
                vector_value float[{online_store.vector_len}]
        );
        """
        )

        # Currently I can only insert the embedding value without crashing SQLite, will report a bug
        cur.execute(
            f"""
            INSERT INTO vec_example(rowid, vector_value)
            select rowid, vector_value from {table_name}
        """
        )
        cur.execute(
            """
            INSERT INTO vec_example(rowid, vector_value)
                VALUES (?, ?)
        """,
            (0, query_embedding_bin),
        )

        # Have to join this with the {table_name} to get the feature name and entity_key
        # Also the `top_k` doesn't appear to be working for some reason
        cur.execute(
            f"""
            select
                fv.entity_key,
                f.vector_value,
                fv.value,
                f.distance,
                fv.event_ts
            from (
                select
                    rowid,
                    vector_value,
                    distance
                from vec_example
                where vector_value match ?
                order by distance
                limit ?
            ) f
            left join {table_name} fv
            on f.rowid = fv.rowid
        """,
            (query_embedding_bin, top_k),
        )

        rows = cur.fetchall()

        result: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[ValueProto],
                Optional[ValueProto],
                Optional[ValueProto],
            ]
        ] = []

        for entity_key, _, string_value, distance, event_ts in rows:
            result.append(
                _build_retrieve_online_document_record(
                    entity_key,
                    string_value if string_value else b"",
                    # This may be a bug
                    embedding,
                    distance,
                    event_ts,
                    config.entity_key_serialization_version,
                )
            )

        return result

    def retrieve_online_documents_v2(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_features: List[str],
        query: List[float],
        top_k: int,
        distance_metric: Optional[str] = None,
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[Dict[str, ValueProto]],
        ]
    ]:
        """
        Retrieve documents using vector similarity search.
        Args:
            config: Feast configuration object
            table: FeatureView object as the table to search
            requested_features: List of requested features to retrieve
            query: Query embedding to search for
            top_k: Number of items to return
            distance_metric: Distance metric to use (optional)
        Returns:
            List of tuples containing the event timestamp, entity key, and feature values
        """
        online_store = config.online_store
        if not isinstance(online_store, SqliteOnlineStoreConfig):
            raise ValueError("online_store must be SqliteOnlineStoreConfig")
        if not online_store.vector_enabled:
            raise ValueError("Vector search is not enabled in the online store config")

        conn = self._get_conn(config)
        cur = conn.cursor()

        online_store = config.online_store
        if not isinstance(online_store, SqliteOnlineStoreConfig):
            raise ValueError("online_store must be SqliteOnlineStoreConfig")
        if not online_store.vector_len:
            raise ValueError("vector_len is not configured in the online store config")
        query_embedding_bin = serialize_f32(query, online_store.vector_len)  # type: ignore
        table_name = _table_id(config.project, table)

        cur.execute(
            f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS vec_example using vec0(
                vector_value float[{online_store.vector_len}]
            );
            """
        )

        cur.execute(
            f"""
            INSERT INTO vec_example(rowid, vector_value)
            select rowid, vector_value from {table_name}
            """
        )

        cur.execute(
            """
            INSERT INTO vec_example(rowid, vector_value)
            VALUES (?, ?)
            """,
            (0, query_embedding_bin),
        )

        cur.execute(
            f"""
            select
                fv.entity_key,
                fv.feature_name,
                fv.value,
                f.distance,
                fv.event_ts,
                fv.created_ts
            from (
                select
                    rowid,
                    vector_value,
                    distance
                from vec_example
                where vector_value match ?
                order by distance
                limit ?
            ) f
            left join {table_name} fv
            on f.rowid = fv.rowid
            where fv.feature_name in ({",".join(["?" for _ in requested_features])})
            """,
            (
                query_embedding_bin,
                top_k,
                *[f.split(":")[-1] for f in requested_features],
            ),
        )

        rows = cur.fetchall()
        result: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []

        for entity_key, feature_name, value_bin, distance, event_ts, created_ts in rows:
            val = ValueProto()
            val.ParseFromString(value_bin)
            entity_key_proto = None
            if entity_key:
                entity_key_proto = deserialize_entity_key(
                    entity_key,
                    entity_key_serialization_version=config.entity_key_serialization_version,
                )
            res = {feature_name: val}
            res["distance"] = ValueProto(float_val=distance)
            result.append((event_ts, entity_key_proto, res))

        return result


def _initialize_conn(db_path: str):
    try:
        import sqlite_vec  # noqa: F401
    except ModuleNotFoundError:
        logging.warning("Cannot use sqlite_vec for vector search")
    Path(db_path).parent.mkdir(exist_ok=True)
    return sqlite3.connect(
        db_path,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        check_same_thread=False,
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
    conn: sqlite3.Connection

    def __init__(self, path: str, name: str):
        super().__init__(name)
        self.path = path
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
        return SqliteTable(
            path=sqlite_table_proto.path,
            name=sqlite_table_proto.name,
        )

    def update(self):
        if sys.version_info[0:2] == (3, 10):
            try:
                import sqlite_vec  # noqa: F401

                self.conn.enable_load_extension(True)
                sqlite_vec.load(self.conn)
            except ModuleNotFoundError:
                logging.warning("Cannot use sqlite_vec for vector search")
        self.conn.execute(
            f"CREATE TABLE IF NOT EXISTS {self.name} (entity_key BLOB, feature_name TEXT, value BLOB, vector_value BLOB, event_ts timestamp, created_ts timestamp,  PRIMARY KEY(entity_key, feature_name))"
        )
        self.conn.execute(
            f"CREATE INDEX IF NOT EXISTS {self.name}_ek ON {self.name} (entity_key);"
        )

    def teardown(self):
        self.conn.execute(f"DROP TABLE IF EXISTS {self.name}")
