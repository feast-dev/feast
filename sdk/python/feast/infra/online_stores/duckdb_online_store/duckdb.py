from datetime import datetime
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple, Sequence

import duckdb

from feast import Entity
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.helpers import _table_id, _to_naive_utc
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.online_stores.vector_store import VectorStoreConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.utils import _build_retrieve_online_document_record

# For more information on the duckdb distance metrics, see:
# https://duckdb.org/docs/extensions/vss
SUPPORTED_DISTANCE_METRICS_DICT = {
    "l2sq": "array_distance",
    "cosine": "array_cosine_distance",
    "ip": "array_negative_inner_product",
}


class DuckDBOnlineStoreConfig(FeastConfigBaseModel, VectorStoreConfig):
    type: Literal["duckdb"] = "duckdb"
    path: str


class DuckDBOnlineStore(OnlineStore):
    _conn: Optional[duckdb.DuckDBPyConnection] = None

    def _get_conn(self, config: RepoConfig) -> duckdb.DuckDBPyConnection:
        if not self._conn:
            self._conn = duckdb.connect(config.online_store.path)
        return self._conn

    def _close_conn(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]] = None,
    ) -> None:
        conn = self._get_conn(config)
        insert_values = []
        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=config.entity_key_serialization_version,
            )
            timestamp = _to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = _to_naive_utc(created_ts)

            for feature_name, val in values.items():
                insert_values.append(
                    (
                        entity_key_bin,
                        feature_name,
                        val.SerializeToString(),
                        timestamp,
                        created_ts,
                    )
                )

        sql_query = f"""
            INSERT INTO {_table_id(config.project, table)}
            (entity_key, feature_name, value, event_ts, created_ts)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (entity_key, feature_name) DO UPDATE SET
                value = excluded.value,
                event_ts = excluded.event_ts,
                created_ts = excluded.created_ts;
        """

        try:
            conn.executemany(sql_query, insert_values)
            if progress:
                progress(len(data))
        except Exception as e:
            raise RuntimeError(f"Failed to write batch to DuckDB: {e}")
        finally:
            self._close_conn()

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        conn = self._get_conn(config)
        keys = [
            serialize_entity_key(entity_key, config.entity_key_serialization_version)
            for entity_key in entity_keys
        ]
        query = f"""
            SELECT entity_key, feature_name, value, event_ts
            FROM {_table_id(config.project, table)}
            WHERE entity_key IN ({", ".join(["?"] * len(keys))})
        """
        try:
            rows = conn.execute(query, keys).fetchall()
            return self._process_rows(keys, rows)
        except Exception as e:
            raise RuntimeError(f"Failed to read from DuckDB: {e}")
        finally:
            self._close_conn()

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
        online_store_config = config.online_store
        vector_enabled = online_store_config.vector_config.vector_enabled
        try:
            for table in tables_to_delete:
                table_name = _table_id(config.project, table)
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            for table in tables_to_keep:
                table_name = _table_id(config.project, table)
                conn.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        entity_key BLOB,
                        feature_name TEXT,
                        value BLOB,
                        event_ts TIMESTAMP,
                        created_ts TIMESTAMP,
                        PRIMARY KEY(entity_key, feature_name)
                    )
                """)
                if vector_enabled:
                    self.create_index(config, table)
        except Exception as e:
            raise RuntimeError(f"Failed to update DuckDB: {e}")
        finally:
            self._close_conn()

    def teardown(
        self,
        config: RepoConfig,
        tables: List[FeatureView],
        entities: List[Entity],
    ):
        conn = self._get_conn(config)
        try:
            for table in tables:
                table_name = _table_id(config.project, table)
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception as e:
            raise RuntimeError(f"Failed to teardown DuckDB: {e}")
        finally:
            self._close_conn()

    def retrieve_online_documents(
        self,
        config: RepoConfig,
        table: FeatureView,
        requested_feature: str,
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
        if distance_metric:
            raise ValueError(
                "Distance Metric is not supported at retrieval time for DuckDB. Please specify the distance metric in the online store config."
            )
        distance_metric = config.online_store.vector_config.similarity

        distance_metric_sql = SUPPORTED_DISTANCE_METRICS_DICT[distance_metric]
        conn = self._get_conn(config)
        table_name = _table_id(config.project, table)

        try:
            query = f"""
                SELECT
                    entity_key,
                    feature_name,
                    value,
                    vector_value,
                    {distance_metric_sql}(vector_value, ?) as distance,
                    event_ts
                FROM {table_name}
                WHERE feature_name = ?
                ORDER BY distance
                LIMIT ?
            """
            rows = conn.execute(query, (embedding, requested_feature, top_k)).fetchall()
            result = []
            for row in rows:
                result.append(
                    _build_retrieve_online_document_record(
                        row[0],
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        config.entity_key_serialization_version,
                    )
                )
            return result
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve online documents from DuckDB: {e}")
        finally:
            self._close_conn()

    def create_index(self, config: RepoConfig, table: FeatureView):
        conn = self._get_conn(config)
        table_name = _table_id(config.project, table)
        online_store_config = config.online_store
        distance_metric = online_store_config.vector_config.similarity
        try:
            conn.execute(f"""
                CREATE INDEX {self.index_name} ON {table_name} USING HNSW(vector_value) WITH (metric = '{distance_metric}')
            """)
        except Exception as e:
            raise RuntimeError(f"Failed to create index in DuckDB: {e}")
        finally:
            self._close_conn()

    def _process_rows(
        self,
        keys: List[bytes],
        rows: List[Tuple[bytes, str, bytes, datetime]],
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        result = []
        for key in keys:
            row_dict = {}
            for row in rows:
                if row[0] == key:
                    row_dict[row[1]] = ValueProto.FromString(row[2])
            if row_dict:
                result.append((row[3], row_dict))
            else:
                result.append((None, None))
        return result
