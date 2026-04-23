"""
MongoDB DataSourceCreator implementation for universal Feast tests.
"""

import os
import tempfile
from typing import Any, Dict, Optional

import pandas as pd
import pytest
from testcontainers.mongodb import MongoDbContainer

from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb import (
    MongoDBOfflineStoreConfig,
    MongoDBSource,
)
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from tests.universal.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None  # type: ignore


class MongoDBDataSourceCreator(DataSourceCreator):
    """DataSourceCreator for the MongoDB offline store (single shared collection)."""

    ENTITY_KEY_VERSION = 3

    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        self.container = MongoDbContainer(
            "mongo:7.0",
            username="test",
            password="test",  # pragma: allowlist secret
        ).with_exposed_ports(27017)
        self.container.start()
        self.port = self.container.get_exposed_port(27017)
        self.connection_string = (
            f"mongodb://test:test@localhost:{self.port}"  # pragma: allowlist secret
        )
        self.database = f"feast_test_{project_name}"
        self.collection = "feature_history"
        self._entity_key_columns: Dict[str, list] = {}

    def _serialize_entity_key(self, row: pd.Series, join_keys: list[str]) -> bytes:
        """Serialize entity key columns to bytes.

        Join keys are sorted before serialization to match the order used by
        _serialize_entity_key_from_row at query time.  Without sorting, compound
        join keys written in a different order would produce different bytes and
        cause all features to be returned as NULL.
        """
        entity_key = EntityKeyProto()
        for key in sorted(join_keys):
            entity_key.join_keys.append(key)
            val = ValueProto()
            value = row[key]
            # bool must be checked before int: bool is a subclass of int
            if isinstance(value, bool):
                val.bool_val = value
            elif isinstance(value, int):
                val.int64_val = value
            elif isinstance(value, str):
                val.string_val = value
            elif isinstance(value, float):
                val.double_val = value
            else:
                val.string_val = str(value)
            entity_key.entity_values.append(val)
        return serialize_entity_key(entity_key, self.ENTITY_KEY_VERSION)

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column: str = "created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = "ts",
    ) -> DataSource:
        """Create a MongoDB data source by inserting df into the shared collection.

        The data is transformed into the One schema:
        - entity_id: serialized entity key
        - feature_view: destination_name
        - features: nested dict of feature values
        - event_timestamp: from timestamp_field
        - created_at: from created_timestamp_column
        """
        # Determine which columns are join keys vs features
        # Join keys must be integer or string types (serializable as entity keys)
        timestamp_cols = {timestamp_field, created_timestamp_column}
        all_cols = set(df.columns) - timestamp_cols - {None}

        # Heuristic: identify join keys (columns ending with _id or known key names)
        join_keys = []
        for c in all_cols:
            if c.endswith("_id") or c in {"driver", "customer", "entity"}:
                dtype = df[c].dtype
                if dtype in ("int64", "int32", "object") or str(dtype).startswith(
                    "int"
                ):
                    join_keys.append(c)

        if not join_keys:
            for c in all_cols:
                if df[c].dtype in ("int64", "int32") or str(df[c].dtype).startswith(
                    "int"
                ):
                    join_keys = [c]
                    break

        feature_cols = [c for c in all_cols if c not in join_keys]
        self._entity_key_columns[destination_name] = join_keys

        docs = []
        for _, row in df.iterrows():
            entity_id = self._serialize_entity_key(row, join_keys)
            features = {col: row[col] for col in feature_cols if pd.notna(row.get(col))}
            doc: Dict[str, Any] = {
                "entity_id": entity_id,
                "feature_view": destination_name,
                "features": features,
            }
            if timestamp_field and timestamp_field in row:
                doc["event_timestamp"] = row[timestamp_field]
            if created_timestamp_column and created_timestamp_column in row:
                doc["created_at"] = row[created_timestamp_column]
            docs.append(doc)

        # Insert into MongoDB
        client: Any = MongoClient(self.connection_string, tz_aware=True)
        try:
            coll = client[self.database][self.collection]
            if docs:
                coll.insert_many(docs)
        finally:
            client.close()

        return MongoDBSource(
            name=destination_name,
            timestamp_field="event_timestamp",
            created_timestamp_column="created_at" if created_timestamp_column else None,
            field_mapping=field_mapping,
        )

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return MongoDBOfflineStoreConfig(
            connection_string=self.connection_string,
            database=self.database,
            collection=self.collection,
        )

    def create_saved_dataset_destination(self) -> SavedDatasetFileStorage:
        # Saved datasets are written as parquet via SavedDatasetFileStorage.
        # A fresh temp file is created per call; teardown() does not clean it up
        # because the test framework owns the lifetime of saved datasets.
        path = os.path.join(
            tempfile.mkdtemp(), f"{self.project_name}_saved_dataset.parquet"
        )
        return SavedDatasetFileStorage(path=path)

    def create_logged_features_destination(self) -> LoggingDestination:
        raise NotImplementedError("MongoDB LoggingDestination not implemented.")

    def teardown(self):
        try:
            client: Any = MongoClient(self.connection_string, tz_aware=True)
            try:
                client[self.database][self.collection].drop()
            finally:
                client.close()
        except Exception:
            pass
        self.container.stop()

    @staticmethod
    def test_markers() -> list:
        return [pytest.mark.mongodb]
