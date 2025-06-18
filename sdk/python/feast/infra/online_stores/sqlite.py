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
from datetime import date, datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
)

from pydantic import StrictStr

from feast import Entity
from feast.feature_view import FeatureView
from feast.field import Field
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
from feast.type_map import feast_value_type_to_python_type
from feast.types import FEAST_VECTOR_TYPES, PrimitiveFeastType
from feast.utils import (
    _build_retrieve_online_document_record,
    _get_feature_view_vector_field_metadata,
    _serialize_vector_to_float_list,
    to_naive_utc,
)


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

    text_search_enabled: bool = False


class SqliteOnlineStore(OnlineStore):
    """
    SQLite implementation of the online store interface. Not recommended for production usage.

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
        enable_sqlite_vec = (
            sys.version_info[0:2] == (3, 10) and config.online_store.vector_enabled
        )
        if not self._conn:
            db_path = self._get_db_path(config)
            self._conn = _initialize_conn(db_path, enable_sqlite_vec)

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
        conn = self._get_conn(config)
        project = config.project
        feature_type_dict = {f.name: f.dtype for f in table.features}
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
                    if config.online_store.vector_enabled:
                        if (
                            feature_type_dict.get(feature_name, None)
                            in FEAST_VECTOR_TYPES
                        ):
                            vector_field_length = getattr(
                                _get_feature_view_vector_field_metadata(table),
                                "vector_length",
                                512,
                            )
                            val_bin = serialize_f32(
                                val.float_list_val.val, vector_field_length
                            )  # type: ignore
                        else:
                            val_bin = feast_value_type_to_python_type(val)
                        conn.execute(
                            f"""
                            INSERT INTO {table_name} (entity_key, feature_name, value, vector_value, event_ts, created_ts)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(entity_key, feature_name) DO UPDATE SET
                                value = excluded.value,
                                vector_value = excluded.vector_value,
                                event_ts = excluded.event_ts,
                                created_ts = excluded.created_ts;
                            """,
                            (
                                entity_key_bin,  # entity_key
                                feature_name,  # feature_name
                                val.SerializeToString(),  # value
                                val_bin,  # vector_value
                                timestamp,  # event_ts
                                created_ts,  # created_ts
                            ),
                        )
                    else:
                        conn.execute(
                            f"""
                            INSERT INTO {table_name} (entity_key, feature_name, value, event_ts, created_ts)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(entity_key, feature_name) DO UPDATE SET
                                value = excluded.value,
                                event_ts = excluded.event_ts,
                                created_ts = excluded.created_ts;
                            """,
                            (
                                entity_key_bin,  # entity_key
                                feature_name,  # feature_name
                                val.SerializeToString(),  # value
                                timestamp,  # event_ts
                                created_ts,  # created_ts
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

        serialized_entity_keys = [
            serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            for entity_key in entity_keys
        ]
        # Fetch all entities in one go
        cur.execute(
            f"SELECT entity_key, feature_name, value, event_ts "
            f"FROM {_table_id(config.project, table)} "
            f"WHERE entity_key IN ({','.join('?' * len(entity_keys))}) "
            f"ORDER BY entity_key",
            serialized_entity_keys,
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
                ts = cast(datetime, ts)
                if ts.tzinfo is not None:
                    res_ts = ts.astimezone(timezone.utc)
                else:
                    res_ts = ts.replace(tzinfo=timezone.utc)

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
        requested_features: List[str],
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
            requested_features: The list of requested features to retrieve
            embedding: The query embedding to search for
            top_k: The number of items to return
        Returns:
            List of tuples containing the event timestamp, the document feature, the vector value, and the distance
        """
        project = config.project

        if not config.online_store.vector_enabled:
            raise ValueError("sqlite-vss is not enabled in the online store config")

        conn = self._get_conn(config)
        cur = conn.cursor()

        vector_field_length = getattr(
            _get_feature_view_vector_field_metadata(table), "vector_length", 512
        )

        # Convert the embedding to a binary format instead of using SerializeToString()
        query_embedding_bin = serialize_f32(embedding, vector_field_length)
        table_name = _table_id(project, table)
        vector_field = _get_vector_field(table)

        cur.execute(
            f"""
            CREATE VIRTUAL TABLE vec_table using vec0(
                vector_value float[{vector_field_length}]
        );
        """
        )

        # Currently I can only insert the embedding value without crashing SQLite, will report a bug
        cur.execute(
            f"""
            INSERT INTO vec_table(rowid, vector_value)
            select rowid, vector_value from {table_name}
            where feature_name = "{vector_field}"
        """
        )
        cur.execute(
            f"""
            CREATE VIRTUAL TABLE IF NOT EXISTS vec_table using vec0(
                vector_value float[{vector_field_length}]
            );
            """
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
                from vec_table
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
        query: Optional[List[float]],
        top_k: int,
        distance_metric: Optional[str] = None,
        query_string: Optional[str] = None,
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
            query: Query embedding to search for (optional)
            top_k: Number of items to return
            distance_metric: Distance metric to use (optional)
            query_string: The query string to search for using keyword search (bm25) (optional)
        Returns:
            List of tuples containing the event timestamp, entity key, and feature values
        """
        online_store = config.online_store
        if not isinstance(online_store, SqliteOnlineStoreConfig):
            raise ValueError("online_store must be SqliteOnlineStoreConfig")
        if not online_store.vector_enabled and not online_store.text_search_enabled:
            raise ValueError(
                "You must enable either vector search or text search in the online store config"
            )

        conn = self._get_conn(config)
        cur = conn.cursor()

        vector_field_length = getattr(
            _get_feature_view_vector_field_metadata(table), "vector_length", 512
        )

        table_name = _table_id(config.project, table)
        vector_field = _get_vector_field(table)

        if online_store.vector_enabled:
            query_embedding_bin = serialize_f32(query, vector_field_length)  # type: ignore
            cur.execute(
                f"""
                CREATE VIRTUAL TABLE IF NOT EXISTS vec_table using vec0(
                    vector_value float[{vector_field_length}]
                );
                """
            )
            cur.execute(
                f"""
                INSERT INTO vec_table (rowid, vector_value)
                select rowid, vector_value from {table_name}
                where feature_name = "{vector_field}"
                """
            )
        elif online_store.text_search_enabled:
            string_field_list = [
                f.name for f in table.features if f.dtype == PrimitiveFeastType.STRING
            ]
            string_fields = ", ".join(string_field_list)
            # TODO: swap this for a value configurable in each Field()
            BM25_DEFAULT_WEIGHTS = ", ".join(
                [
                    str(1.0)
                    for f in table.features
                    if f.dtype == PrimitiveFeastType.STRING
                ]
            )
            cur.execute(
                f"""
                CREATE VIRTUAL TABLE IF NOT EXISTS search_table using fts5(
                    entity_key, fv_rowid, {string_fields}, tokenize="porter unicode61"
                );
                """
            )
            insert_query = _generate_bm25_search_insert_query(
                table_name, string_field_list
            )
            cur.execute(insert_query)

        else:
            raise ValueError(
                "Neither vector search nor text search are enabled in the online store config"
            )

        if online_store.vector_enabled:
            cur.execute(
                f"""
                select
                    fv2.entity_key,
                    fv2.feature_name,
                    fv2.value,
                    fv.vector_value,
                    f.distance,
                    fv.event_ts,
                    fv.created_ts
                from (
                    select
                        rowid,
                        vector_value,
                        distance
                    from vec_table
                    where vector_value match ?
                    order by distance
                    limit ?
                ) f
                left join {table_name} fv
                    on f.rowid = fv.rowid
                left join {table_name} fv2
                    on fv.entity_key = fv2.entity_key
                where fv2.feature_name != "{vector_field}"
                """,
                (
                    query_embedding_bin,
                    top_k,
                ),
            )
        elif online_store.text_search_enabled:
            cur.execute(
                f"""
            select
                fv.entity_key,
                fv.feature_name,
                fv.value,
                fv.vector_value,
                f.distance,
                fv.event_ts,
                fv.created_ts
                from {table_name} fv
                inner join (
                    select
                        fv_rowid,
                        entity_key,
                        {string_fields},
                        bm25(search_table, {BM25_DEFAULT_WEIGHTS}) as distance
                    from search_table
                    where search_table match ? order by distance limit ?
                ) f
                    on f.entity_key = fv.entity_key
                """,
                (query_string, top_k),
            )

        else:
            raise ValueError(
                "Neither vector search nor text search are enabled in the online store config"
            )

        rows = cur.fetchall()
        results: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[Dict[str, ValueProto]],
            ]
        ] = []

        entity_dict: Dict[
            str, Dict[str, Union[str, ValueProto, EntityKeyProto, datetime]]
        ] = {}
        for (
            entity_key,
            feature_name,
            value_bin,
            vector_value,
            distance,
            event_ts,
            created_ts,
        ) in rows:
            entity_key_proto = deserialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            if entity_key not in entity_dict:
                entity_dict[entity_key] = {}

            feature_val = ValueProto()
            feature_val.ParseFromString(value_bin)
            entity_dict[entity_key]["entity_key_proto"] = entity_key_proto
            entity_dict[entity_key][feature_name] = feature_val
            if online_store.vector_enabled:
                entity_dict[entity_key][vector_field] = _serialize_vector_to_float_list(
                    vector_value
                )
            entity_dict[entity_key]["distance"] = ValueProto(float_val=distance)
            entity_dict[entity_key]["event_ts"] = event_ts
            entity_dict[entity_key]["created_ts"] = created_ts

        for entity_key_value in entity_dict:
            res_event_ts: Optional[datetime] = None
            res_entity_key_proto: Optional[EntityKeyProto] = None
            if isinstance(entity_dict[entity_key_value]["event_ts"], datetime):
                res_event_ts = entity_dict[entity_key_value]["event_ts"]  # type: ignore[assignment]

            if isinstance(
                entity_dict[entity_key_value]["entity_key_proto"], EntityKeyProto
            ):
                res_entity_key_proto = entity_dict[entity_key_value]["entity_key_proto"]  # type: ignore[assignment]

            res_dict: Dict[str, ValueProto] = {
                k: v
                for k, v in entity_dict[entity_key_value].items()
                if isinstance(v, ValueProto) and isinstance(k, str)
            }

            results.append(
                (
                    res_event_ts,
                    res_entity_key_proto,
                    res_dict,
                )
            )
        return results


def _initialize_conn(
    db_path: str, enable_sqlite_vec: bool = False
) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(exist_ok=True)
    db = sqlite3.connect(
        db_path,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        check_same_thread=False,
    )
    if enable_sqlite_vec:
        try:
            import sqlite_vec  # noqa: F401
        except ModuleNotFoundError:
            logging.warning("Cannot use sqlite_vec for vector search")

        db.enable_load_extension(True)
        sqlite_vec.load(db)

    return db


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
            f"""
            CREATE TABLE IF NOT EXISTS {self.name} (
                entity_key BLOB,
                feature_name TEXT,
                value BLOB,
                vector_value BLOB,
                event_ts timestamp,
                created_ts timestamp,
                PRIMARY KEY(entity_key, feature_name)
            )
            """
        )
        self.conn.execute(
            f"CREATE INDEX IF NOT EXISTS {self.name}_ek ON {self.name} (entity_key);"
        )

    def teardown(self):
        self.conn.execute(f"DROP TABLE IF EXISTS {self.name}")


def _get_vector_field(table: FeatureView) -> str:
    """
    Get the vector field from the feature view. There can be only one.
    """
    vector_fields: List[Field] = [
        f for f in table.features if getattr(f, "vector_index", None)
    ]
    assert len(vector_fields) > 0, (
        f"No vector field found, please update feature view = {table.name} to declare a vector field"
    )
    assert len(vector_fields) < 2, (
        "Only one vector field is supported, please update feature view = {table.name} to declare one vector field"
    )
    vector_field: str = vector_fields[0].name
    return vector_field


def _generate_bm25_search_insert_query(
    table_name: str, string_field_list: List[str]
) -> str:
    """
    Generates an SQL insertion query for the given table and string fields.

    Args:
        table_name (str): The name of the table to select data from.
        string_field_list (List[str]): The list of string fields to be used in the insertion.

    Returns:
        str: The generated SQL insertion query.
    """
    _string_fields = ", ".join(string_field_list)
    query = f"INSERT INTO search_table (entity_key, fv_rowid, {_string_fields})\nSELECT\n\tDISTINCT fv0.entity_key,\n\tfv0.rowid as fv_rowid"
    from_query = f"\nFROM (select rowid, * from {table_name} where feature_name = '{string_field_list[0]}') fv0"

    for i, string_field in enumerate(string_field_list):
        query += f"\n\t,fv{i}.value as {string_field}"
        if i > 0:
            from_query += (
                f"\nLEFT JOIN (select rowid, * from {table_name} where feature_name = '{string_field}') fv{i}"
                + f"\n\tON fv0.entity_key = fv{i}.entity_key"
            )

    return query + from_query
