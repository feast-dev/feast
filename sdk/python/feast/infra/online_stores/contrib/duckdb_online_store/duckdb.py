import abc
import contextlib
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import duckdb
from infra.key_encoding_utils import serialize_entity_key
from utils import (
    _build_retrieve_online_document_record,
)

from feast.feature_view import FeatureView
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig


class DuckDBOnlineStoreConfig:
    type: str = "duckdb"
    path: str
    read_only: bool = False
    enable_vector_search: bool = False  # New option for enabling vector search
    dimension: Optional[int] = 512
    distance_metric: Optional[str] = "L2"


class DuckDBOnlineStore(OnlineStore):
    __conn: Optional[duckdb.Connection] = None

    @abc.abstractmethod
    async def online_read_async(
            self,
            config: RepoConfig,
            table: FeatureView,
            entity_keys: List[EntityKeyProto],
            requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        pass

    @contextlib.contextmanager
    def _get_conn(self,
                  config: RepoConfig) -> Any:
        assert config.online_store.type == "duckdb"
        online_store_config = config.online_store

        if self.__conn is None:
            self.__conn = duckdb.connect(database=online_store_config.path, read_only=online_store_config.read_only)
        yield self.__conn

    def create_vector_index(
            self,
            config: RepoConfig,
            table_name: str,
            vector_column: str
    ) -> None:
        """Create an HNSW index for vector similarity search."""
        if not config.online_store.enable_vector_search:
            raise ValueError("Vector search is not enabled in the configuration.")
        distance_metric = config.online_store.distance_metric

        with self._get_conn(None) as conn:
            conn.execute(
                f"CREATE INDEX idx ON {table_name} USING HNSW ({vector_column}) WITH (metric = '{distance_metric}');"
            )

    def online_write_batch(
            self,
            config: RepoConfig,
            table: FeatureView,
            data: List[Tuple[EntityKeyProto, Dict[str, ValueProto]]],
    ) -> None:
        insert_values = []
        for entity_key, values in data:
            entity_key_bin = serialize_entity_key(entity_key).hex()
            for feature_name, val in values.items():
                insert_values.append(
                    (entity_key_bin, feature_name, val.SerializeToString())
                )

        with self._get_conn(config) as conn:
            conn.execute(
                f"CREATE TABLE IF NOT EXISTS {table.name} (entity_key BLOB, feature_name TEXT, value BLOB)"
            )
            conn.executemany(
                f"INSERT INTO {table.name} (entity_key, feature_name, value) VALUES (?, ?, ?)",
                insert_values,
            )

    def online_read(
            self,
            config: RepoConfig,
            table: FeatureView,
            entity_keys: List[EntityKeyProto],
            requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[Dict[str, ValueProto]]]]:
        keys = [serialize_entity_key(key).hex() for key in entity_keys]
        query = f"SELECT feature_name, value FROM {table.name} WHERE entity_key IN ({','.join(['?'] * len(keys))})"

        with self._get_conn(config) as conn:
            results = conn.execute(query, keys).fetchall()

        return [
            {
                feature_name: ValueProto().ParseFromString(value)
                for feature_name, value in results
            }
        ]

    def retrieve_online_documents(
            self,
            config: RepoConfig,
            table: FeatureView,
            requested_feature: str,
            embedding: List[float],
            top_k: int,
            distance_metric: Optional[str] = "L2",
    ) -> List[
        Tuple[
            Optional[datetime],
            Optional[EntityKeyProto],
            Optional[ValueProto],
            Optional[ValueProto],
            Optional[ValueProto],
        ]
    ]:
        """Perform a vector similarity search using the HNSW index."""
        if not self.config.enable_vector_search:
            raise ValueError("Vector search is not enabled in the configuration.")
        if config.entity_key_serialization_version < 3:
            raise ValueError(
                "Entity key serialization version must be at least 3 for vector search."
            )

        result: List[
            Tuple[
                Optional[datetime],
                Optional[EntityKeyProto],
                Optional[ValueProto],
                Optional[ValueProto],
                Optional[ValueProto],
            ]
        ] = []

        with self._get_conn(config) as conn:
            query = f"""
            SELECT
                entity_key,
                feature_name,
                value,
                vector_value,
                event_ts
            FROM {table.name}
            WHERE feature_name = '{requested_feature}'
            ORDER BY array_distance(vec, ?::FLOAT[]) LIMIT ?;
            """
            rows = conn.execute(query, (embedding, top_k)).fetchall()
            for (
                    entity_key,
                    _,
                    feature_val,
                    vector_value,
                    distance_val,
                    event_ts,
            ) in rows:
                result.append(
                    _build_retrieve_online_document_record(
                        entity_key=entity_key,
                        feature_value=feature_val,
                        vector_value=vector_value,
                        distance_value=distance_val,
                        event_timestamp=event_ts,
                        entity_key_serialization_version=config.entity_key_serialization_version,
                    )
                )

        return result

    def update(
            self,
            config: RepoConfig,
            tables_to_delete: List[FeatureView],
            tables_to_keep: List[FeatureView],
    ) -> None:
        with self._get_conn(config) as conn:
            for table in tables_to_delete:
                conn.execute(f"DROP TABLE IF EXISTS {table.name}")
            for table in tables_to_keep:
                conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {table.name} (entity_key BLOB, feature_name TEXT, value BLOB)"
                )

    def teardown(
            self,
            config: RepoConfig,
            tables: List[FeatureView],
    ) -> None:
        with self._get_conn(config) as conn:
            for table in tables:
                conn.execute(f"DROP TABLE IF EXISTS {table.name}")
