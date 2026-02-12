# Copyright 2025 The Feast Authors
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

"""
Unit and integration tests for MongoDB Online Store.

These tests cover:
- Configuration validation
- CRUD operations (sync and async)
- Index creation
- TTL functionality
- Connection management
"""

import os
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from feast import Entity, FeatureView, Field, FileSource, RepoConfig
from feast.infra.online_stores.mongodb_online_store.mongodb import (
    MongoDBOnlineStore,
    MongoDBOnlineStoreConfig,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Float32, Int64, String
from feast.value_type import ValueType

MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://localhost:27017")

class TestMongoDBOnlineStoreConfig:
    """Test suite for MongoDBOnlineStoreConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = MongoDBOnlineStoreConfig(
            connection_string=MONGODB_URI
        )

        assert config.type == "mongodb"
        assert config.database == "feast"
        assert config.max_pool_size == 50
        assert config.min_pool_size == 10
        assert config.ttl_seconds is None
        assert config.atlas_vector_search_enabled is False
        assert config.vector_index_name == "vector_index"
        assert config.vector_similarity_metric == "cosine"
        assert config.write_concern_w == 1
        assert config.read_preference == "primaryPreferred"

    def test_custom_config(self):
        """Test custom configuration values."""
        config = MongoDBOnlineStoreConfig(
            connection_string="mongodb+srv://user:pass@cluster.mongodb.net",
            database="custom_db",
            max_pool_size=100,
            min_pool_size=20,
            ttl_seconds=86400,
            atlas_vector_search_enabled=True,
            vector_similarity_metric="euclidean",
            write_concern_w=2,
            read_preference="primary",
        )

        assert config.database == "custom_db"
        assert config.max_pool_size == 100
        assert config.min_pool_size == 20
        assert config.ttl_seconds == 86400
        assert config.atlas_vector_search_enabled is True
        assert config.vector_similarity_metric == "euclidean"
        assert config.write_concern_w == 2
        assert config.read_preference == "primary"

    def test_atlas_connection_string(self):
        """Test Atlas connection string format."""
        config = MongoDBOnlineStoreConfig(
            connection_string="mongodb+srv://cluster.mongodb.net"
        )
        assert "mongodb+srv://" in config.connection_string


class TestMongoDBOnlineStore:
    """Test suite for MongoDBOnlineStore implementation."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock RepoConfig for testing."""
        return RepoConfig(
            project="test_project",
            online_store=MongoDBOnlineStoreConfig(
                connection_string=MONGODB_URI,
                database="feast_test",
            ),
            registry="dummy_registry",
        )

    @pytest.fixture
    def mock_config_with_ttl(self):
        """Create a mock RepoConfig with TTL enabled."""
        return RepoConfig(
            project="test_project",
            online_store=MongoDBOnlineStoreConfig(
                connection_string=MONGODB_URI,
                database="feast_test",
                ttl_seconds=86400,
            ),
            registry="dummy_registry",
        )

    @pytest.fixture
    def feature_view(self):
        """Create a test FeatureView."""
        entity = Entity(
            name="user_id", description="User ID", value_type=ValueType.INT64
        )
        source = FileSource(path="test.parquet", timestamp_field="event_timestamp")
        return FeatureView(
            name="test_features",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="user_id", dtype=Int64),
                Field(name="feature1", dtype=String),
                Field(name="feature2", dtype=Float32),
            ],
            source=source,
        )

    @pytest.fixture
    def entity_keys(self):
        """Create test entity keys."""
        keys = []
        for i in range(3):
            key = EntityKeyProto()
            key.join_keys.append(f"user_{i}")
            key.entity_values.append(
                ValueProto(int64_val=i)
            )
            keys.append(key)
        return keys

    @pytest.fixture
    def feature_data(self, entity_keys):
        """Create test feature data."""
        data = []
        timestamp = datetime.now(UTC)

        for i, key in enumerate(entity_keys):
            features = {
                "feature1": ValueProto(string_val=f"value_{i}"),
                "feature2": ValueProto(float_val=float(i) * 1.5),
            }
            data.append((key, features, timestamp, None))

        return data

    def test_compute_entity_key_hash(self):
        """Test entity key hashing."""
        store = MongoDBOnlineStore()

        # Test consistent hashing
        key_bin = b"test_entity_key"
        hash1 = store._compute_entity_key_hash(key_bin)
        hash2 = store._compute_entity_key_hash(key_bin)

        assert hash1 == hash2
        assert isinstance(hash1, str)
        assert len(hash1) == 32  # MD5 hash length

    def test_async_supported(self):
        """Test that async operations are supported."""
        store = MongoDBOnlineStore()
        async_methods = store.async_supported

        assert async_methods.read is True
        assert async_methods.write is True

    @patch("feast.infra.online_stores.mongodb_online_store.mongodb.get_mongo_client")
    def test_online_write_batch(self, mock_get_client, mock_config, feature_view, feature_data):
        """Test synchronous write batch operation."""
        # Mock MongoDB client and collection
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()

        # mock_client.__getitem__.return_value = mock_db
        # mock_db.__getitem__.return_value = mock_collection
        # mock_get_client.return_value = mock_client

        # Create store and write data
        store = MongoDBOnlineStore()
        store.online_write_batch(
            config=mock_config,
            table=feature_view,
            data=feature_data,
            progress=None,
        )

        # Verify client was created
        mock_get_client.assert_called_once()

        # Verify bulk_write was called
        mock_collection.bulk_write.assert_called_once()

        # Get the bulk operations
        call_args = mock_collection.bulk_write.call_args
        operations = call_args[0][0]

        # Verify we have the right number of operations
        assert len(operations) == len(feature_data)

    @patch("feast.infra.online_stores.mongodb_online_store.mongodb.get_mongo_client")
    def test_online_write_batch_with_ttl(
        self, mock_get_client, mock_config_with_ttl, feature_view, feature_data
    ):
        """Test write batch with TTL enabled."""
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()

        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_get_client.return_value = mock_client

        store = MongoDBOnlineStore()
        store.online_write_batch(
            config=mock_config_with_ttl,
            table=feature_view,
            data=feature_data,
            progress=None,
        )

        # Verify bulk_write was called
        assert mock_collection.bulk_write.called

    @patch("feast.infra.online_stores.mongodb_online_store.mongodb.get_mongo_client")
    def test_online_read(self, mock_get_client, mock_config, feature_view, entity_keys):
        """Test synchronous read operation."""
        # Mock MongoDB client and collection
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()

        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_get_client.return_value = mock_client

        # Mock find result
        mock_doc = {
            "entity_key_hash": "test_hash",
            "event_timestamp": datetime.now(UTC),
            "features": {
                "feature1": ValueProto(string_val="test").SerializeToString(),
                "feature2": ValueProto(float_val=1.5).SerializeToString(),
            },
        }
        mock_collection.find.return_value = [mock_doc]

        # Create store and read data
        store = MongoDBOnlineStore()
        results = store.online_read(
            config=mock_config,
            table=feature_view,
            entity_keys=entity_keys,
            requested_features=None,
        )

        # Verify results
        assert len(results) == len(entity_keys)
        assert mock_collection.find.called

    @patch("feast.infra.online_stores.mongodb_online_store.mongodb.get_mongo_client")
    def test_online_read_with_missing_entities(
        self, mock_get_client, mock_config, feature_view, entity_keys
    ):
        """Test read operation with some missing entities."""
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()

        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_get_client.return_value = mock_client

        # Return empty list (no documents found)
        mock_collection.find.return_value = []

        store = MongoDBOnlineStore()
        results = store.online_read(
            config=mock_config,
            table=feature_view,
            entity_keys=entity_keys,
            requested_features=None,
        )

        # All results should be (None, None) for missing entities
        assert len(results) == len(entity_keys)
        for event_ts, features in results:
            assert event_ts is None
            assert features is None

    @patch("feast.infra.online_stores.mongodb_online_store.mongodb.get_mongo_client")
    def test_update_creates_indexes(self, mock_get_client, mock_config, feature_view):
        """Test that update creates necessary indexes."""
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()

        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_get_client.return_value = mock_client

        store = MongoDBOnlineStore()
        store.update(
            config=mock_config,
            tables_to_delete=[],
            tables_to_keep=[feature_view],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=False,
        )

        # Verify create_index was called for entity_key_hash
        assert mock_collection.create_index.called
        call_args_list = mock_collection.create_index.call_args_list

        # Should have at least one create_index call
        assert len(call_args_list) >= 1

    @patch("feast.infra.online_stores.mongodb_online_store.mongodb.get_mongo_client")
    def test_update_creates_ttl_index(
        self, mock_get_client, mock_config_with_ttl, feature_view
    ):
        """Test that update creates TTL index when TTL is configured."""
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()

        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_get_client.return_value = mock_client

        store = MongoDBOnlineStore()
        store.update(
            config=mock_config_with_ttl,
            tables_to_delete=[],
            tables_to_keep=[feature_view],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=False,
        )

        # Verify create_index was called
        assert mock_collection.create_index.called

    @patch("feast.infra.online_stores.mongodb_online_store.mongodb.get_mongo_client")
    def test_update_deletes_collections(self, mock_get_client, mock_config, feature_view):
        """Test that update deletes collections for removed feature views."""
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()

        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_get_client.return_value = mock_client

        store = MongoDBOnlineStore()
        store.update(
            config=mock_config,
            tables_to_delete=[feature_view],
            tables_to_keep=[],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=False,
        )

        # Verify collection.drop was called
        assert mock_collection.drop.called

    @patch("feast.infra.online_stores.mongodb_online_store.mongodb.get_mongo_client")
    def test_teardown(self, mock_get_client, mock_config, feature_view):
        """Test teardown operation."""
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()

        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_get_client.return_value = mock_client

        store = MongoDBOnlineStore()
        store.teardown(
            config=mock_config,
            tables=[feature_view],
            entities=[],
        )

        # Verify collection.drop was called
        assert mock_collection.drop.called


class TestMongoDBOnlineStoreAsync:
    """Test suite for async operations."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock RepoConfig for testing."""
        return RepoConfig(
            project="test_project",
            online_store=MongoDBOnlineStoreConfig(
                connection_string="mongodb://localhost:27017",
                database="feast_test",
            ),
            registry="dummy_registry",
        )

    @pytest.fixture
    def feature_view(self):
        """Create a test FeatureView."""
        entity = Entity(
            name="user_id", description="User ID", value_type=ValueType.INT64
        )
        source = FileSource(path="test.parquet", timestamp_field="event_timestamp")
        return FeatureView(
            name="test_features",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="user_id", dtype=Int64),
                Field(name="feature1", dtype=String),
            ],
            source=source,
        )

    @pytest.mark.asyncio
    @patch("feast.infra.online_stores.mongodb_online_store.mongodb.get_async_mongo_client")
    async def test_online_write_batch_async(
        self, mock_get_client_async, mock_config, feature_view
    ):
        """Test async write batch operation."""
        # Mock async MongoDB client
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_collection.bulk_write = MagicMock(return_value=None)

        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_get_client_async.return_value = mock_client

        # Create test data
        entity_key = EntityKeyProto()
        entity_key.join_keys.append("user_id")
        entity_key.entity_values.append(ValueProto(int64_val=123))

        features = {
            "feature1": ValueProto(string_val="test"),
        }

        data = [(entity_key, features, datetime.now(UTC), None)]

        # Write data
        store = MongoDBOnlineStore()
        await store.online_write_batch_async(
            config=mock_config,
            table=feature_view,
            data=data,
            progress=None,
        )

        # Verify client was created
        mock_get_client_async.assert_called_once()

    @pytest.mark.asyncio
    @patch("feast.infra.online_stores.mongodb_online_store.mongodb.get_async_mongo_client")
    async def test_online_read_async(
        self, mock_get_client_async, mock_config, feature_view
    ):
        """Test async read operation."""
        # Mock async MongoDB client
        mock_client = MagicMock()
        mock_db = MagicMock()
        mock_collection = MagicMock()

        # Mock cursor
        mock_cursor = MagicMock()
        mock_cursor.to_list = MagicMock(return_value=[])
        mock_collection.find.return_value = mock_cursor

        mock_client.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        mock_get_client_async.return_value = mock_client

        # Create test entity key
        entity_key = EntityKeyProto()
        entity_key.join_keys.append("user_id")
        entity_key.entity_values.append(ValueProto(int64_val=123))

        # Read data
        store = MongoDBOnlineStore()
        results = await store.online_read_async(
            config=mock_config,
            table=feature_view,
            entity_keys=[entity_key],
            requested_features=None,
        )

        # Verify results
        assert len(results) == 1
        mock_collection.find.assert_called_once()


@pytest.mark.integration
@pytest.mark.skipif(
    os.getenv("MONGODB_CONNECTION_STRING") is None,
    reason="MongoDB connection string not provided. Set MONGODB_CONNECTION_STRING env var to run integration tests.",
)
class TestMongoDBOnlineStoreIntegration:
    """
    Integration tests for MongoDB Online Store.

    These tests require a running MongoDB instance.
    Set MONGODB_CONNECTION_STRING environment variable to run these tests.
    Example: export MONGODB_CONNECTION_STRING="mongodb://localhost:27017"
    """

    @pytest.fixture
    def mongodb_config(self):
        """Create configuration using real MongoDB connection."""
        return RepoConfig(
            project="integration_test",
            online_store=MongoDBOnlineStoreConfig(
                connection_string=os.getenv("MONGODB_CONNECTION_STRING"),
                database="feast_integration_test",
            ),
            registry="dummy_registry",
        )

    @pytest.fixture
    def feature_view(self):
        """Create a test FeatureView."""
        entity = Entity(
            name="user_id", description="User ID", value_type=ValueType.INT64
        )
        source = FileSource(path="test.parquet", timestamp_field="event_timestamp")
        return FeatureView(
            name="test_integration_features",
            entities=[entity],
            ttl=timedelta(days=1),
            schema=[
                Field(name="user_id", dtype=Int64),
                Field(name="feature1", dtype=String),
                Field(name="feature2", dtype=Float32),
            ],
            source=source,
        )

    def test_full_write_read_cycle(self, mongodb_config, feature_view):
        """Test a complete write and read cycle with real MongoDB."""
        store = MongoDBOnlineStore()

        # Setup: Create indexes
        store.update(
            config=mongodb_config,
            tables_to_delete=[],
            tables_to_keep=[feature_view],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=False,
        )

        # Create test data
        entity_key = EntityKeyProto()
        entity_key.join_keys.append("user_id")
        entity_key.entity_values.append(ValueProto(int64_val=123))

        features = {
            "feature1": ValueProto(string_val="integration_test_value"),
            "feature2": ValueProto(float_val=42.5),
        }

        timestamp = datetime.now(UTC)
        data = [(entity_key, features, timestamp, None)]

        # Write data
        store.online_write_batch(
            config=mongodb_config,
            table=feature_view,
            data=data,
            progress=None,
        )

        # Read data back
        results = store.online_read(
            config=mongodb_config,
            table=feature_view,
            entity_keys=[entity_key],
            requested_features=None,
        )

        # Verify results
        assert len(results) == 1
        event_ts, feature_dict = results[0]

        assert event_ts is not None
        assert feature_dict is not None
        assert "feature1" in feature_dict
        assert "feature2" in feature_dict
        assert feature_dict["feature1"].string_val == "integration_test_value"
        assert abs(feature_dict["feature2"].float_val - 42.5) < 0.01

        # Cleanup
        store.teardown(
            config=mongodb_config,
            tables=[feature_view],
            entities=[],
        )
