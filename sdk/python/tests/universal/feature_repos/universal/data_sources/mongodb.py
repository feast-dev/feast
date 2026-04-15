"""
MongoDB DataSourceCreator implementations for universal Feast tests.

Provides two implementations matching the two offline store schemas:
- MongoDBManyDataSourceCreator: One collection per FeatureView (Many)
- MongoDBOneDataSourceCreator: Single shared collection (One)
"""

from typing import Any, Dict, Optional

import pandas as pd
import pytest
from testcontainers.mongodb import MongoDbContainer

from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_many import (
    MongoDBOfflineStoreManyConfig,
    MongoDBSourceMany,
    SavedDatasetMongoDBStorageMany,
)
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_one import (
    MongoDBOfflineStoreOneConfig,
    MongoDBSourceOne,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel
from feast.saved_dataset import SavedDatasetStorage
from tests.universal.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)

# Import pymongo - will be available since we're testing MongoDB
try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None  # type: ignore


class MongoDBManyDataSourceCreator(DataSourceCreator):
    """DataSourceCreator for MongoDBOfflineStoreMany (one collection per FeatureView)."""

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
        self.collections_created: list[str] = []

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column: str = "created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = "ts",
    ) -> DataSource:
        """Create a MongoDB data source by inserting df into a collection."""
        collection_name = self.get_prefixed_table_name(destination_name)

        # Insert data into MongoDB
        client: Any = MongoClient(self.connection_string, tz_aware=True)
        try:
            coll = client[self.database][collection_name]
            coll.drop()  # Clean slate
            records = df.to_dict("records")
            if records:
                coll.insert_many(records)
            self.collections_created.append(collection_name)
        finally:
            client.close()

        return MongoDBSourceMany(
            name=destination_name,
            database=self.database,
            collection=collection_name,
            timestamp_field=timestamp_field or "ts",
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
        )

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return MongoDBOfflineStoreManyConfig(
            connection_string=self.connection_string,
            database=self.database,
        )

    def create_saved_dataset_destination(self) -> SavedDatasetStorage:
        return SavedDatasetMongoDBStorageMany(
            database=self.database,
            collection=f"{self.project_name}_saved_dataset",
        )

    def create_logged_features_destination(self) -> LoggingDestination:
        # MongoDB doesn't have a native LoggingDestination yet
        # Return None or raise NotImplementedError for now
        raise NotImplementedError(
            "MongoDB LoggingDestination not implemented. "
            "Tests requiring logging features will be skipped."
        )

    def teardown(self):
        """Clean up: drop collections and stop container."""
        try:
            client: Any = MongoClient(self.connection_string, tz_aware=True)
            try:
                db = client[self.database]
                for coll_name in self.collections_created:
                    db[coll_name].drop()
            finally:
                client.close()
        except Exception:
            pass  # Container may already be stopped
        self.container.stop()

    @staticmethod
    def test_markers() -> list:
        """Mark tests as requiring MongoDB."""
        return [pytest.mark.mongodb]


class MongoDBOneDataSourceCreator(DataSourceCreator):
    """DataSourceCreator for MongoDBOfflineStoreOne (single shared collection).

    This implementation uses the nested features schema where all FeatureViews
    share a single collection with a discriminator field.

    TODO: This DataSourceCreator has a fundamental limitation. The One schema
        requires knowing which columns are join keys vs features to properly
        serialize entity_id and nest features. However, create_data_source() only
        receives a DataFrame and column names - it doesn't have access to Entity
        definitions that specify join keys.

    Current workaround uses heuristics (columns ending in '_id' with int/string
    dtype), which is fragile. A proper fix would require modifying the
    DataSourceCreator interface to pass entity/join key information to
    create_data_source(), which is a Feast core change.

    For now, universal tests may fail for FeatureViews where the heuristic
    doesn't correctly identify join keys. Use unit tests in
    tests/unit/infra/offline_stores/contrib/mongodb_offline_store/test_one.py
    for comprehensive testing of the One implementation.
    """

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
        self.feature_views_created: list[str] = []
        # Track entity key columns per feature view for serialization
        self._entity_key_columns: Dict[str, list[str]] = {}

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
            # bool must be checked before int: bool is a subclass of int in Python
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

        # Heuristic: identify join keys
        # 1. Must end with "_id" or be a known key name
        # 2. Must be integer or string type (not float)
        join_keys = []
        for c in all_cols:
            if c.endswith("_id") or c in {"driver", "customer", "entity"}:
                dtype = df[c].dtype
                # Only integer or string types can be join keys
                if dtype in ("int64", "int32", "object") or str(dtype).startswith(
                    "int"
                ):
                    join_keys.append(c)

        if not join_keys:
            # Fallback: first integer column
            for c in all_cols:
                if df[c].dtype in ("int64", "int32") or str(df[c].dtype).startswith(
                    "int"
                ):
                    join_keys = [c]
                    break

        feature_cols = [c for c in all_cols if c not in join_keys]

        # Store for later use
        self._entity_key_columns[destination_name] = join_keys

        # Transform to One schema
        docs = []
        for _, row in df.iterrows():
            entity_id = self._serialize_entity_key(row, join_keys)
            features = {col: row[col] for col in feature_cols if pd.notna(row.get(col))}

            doc = {
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
            self.feature_views_created.append(destination_name)
        finally:
            client.close()

        return MongoDBSourceOne(
            name=destination_name,
            timestamp_field="event_timestamp",
            created_timestamp_column="created_at" if created_timestamp_column else None,
            field_mapping=field_mapping,
        )

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return MongoDBOfflineStoreOneConfig(
            connection_string=self.connection_string,
            database=self.database,
            collection=self.collection,
        )

    def create_saved_dataset_destination(self) -> SavedDatasetStorage:
        # One implementation doesn't have SavedDatasetStorage yet
        raise NotImplementedError(
            "MongoDBOfflineStoreOne SavedDatasetStorage not implemented."
        )

    def create_logged_features_destination(self) -> LoggingDestination:
        raise NotImplementedError("MongoDB LoggingDestination not implemented.")

    def teardown(self):
        """Clean up: drop the collection and stop container."""
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
        """Mark tests as requiring MongoDB."""
        return [pytest.mark.mongodb]
