import pytest
from unittest.mock import Mock, patch
from feast.infra.online_stores.redis import RedisOnlineStore, RedisOnlineStoreConfig
from feast.repo_config import RepoConfig
from feast import FeatureView, Field, Entity
from feast.types import Array, Float32, Int64
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


@pytest.fixture
def redis_config():
    return RedisOnlineStoreConfig(
        type="redis",
        connection_string="localhost:6379",
        vector_enabled=True,
        vector_dim=128,
        vector_index_type="FLAT",
        vector_distance_metric="COSINE"
    )


@pytest.fixture
def repo_config(redis_config):
    return RepoConfig(
        project="test_project",
        registry="test_registry",
        provider="local",
        online_store=redis_config
    )


@pytest.fixture
def vector_feature_view():
    entity = Entity(name="item", join_keys=["item_id"])
    return FeatureView(
        name="test_embeddings",
        entities=[entity],
        schema=[
            Field(
                name="vector",
                dtype=Array(Float32),
                vector_index=True,
                vector_search_metric="COSINE",
            ),
            Field(name="item_id", dtype=Int64),
        ],
        source=Mock(),
    )


class TestRedisVectorStore:
    def test_config_inheritance(self, redis_config):
        """Test that RedisOnlineStoreConfig properly inherits from VectorStoreConfig."""
        assert hasattr(redis_config, 'vector_enabled')
        assert redis_config.vector_enabled is True
        assert redis_config.vector_dim == 128
        assert redis_config.vector_index_type == "FLAT"
        assert redis_config.vector_distance_metric == "COSINE"

    def test_extract_vector_from_value(self):
        """Test vector extraction from ValueProto."""
        store = RedisOnlineStore()
        
        # Test float list
        val = ValueProto()
        val.float_list_val.val[:] = [1.0, 2.0, 3.0, 4.0]
        
        vector_bytes = store._extract_vector_from_value(val)
        assert vector_bytes is not None
        assert isinstance(vector_bytes, bytes)

    @patch('feast.infra.online_stores.redis.RedisOnlineStore._get_client')
    def test_create_vector_index(self, mock_get_client, repo_config, vector_feature_view):
        """Test vector index creation."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        
        # Mock that index doesn't exist
        mock_client.execute_command.side_effect = [Exception("Index not found"), None]
        
        store = RedisOnlineStore()
        store._create_vector_index(repo_config, vector_feature_view)
        
        # Verify that FT.CREATE was called
        assert mock_client.execute_command.call_count == 2
        create_call = mock_client.execute_command.call_args_list[1]
        assert create_call[0][0] == "FT.CREATE"

    @patch('feast.infra.online_stores.redis.RedisOnlineStore._get_client')
    def test_retrieve_online_documents_v2_validation(self, mock_get_client, repo_config, vector_feature_view):
        """Test validation in retrieve_online_documents_v2."""
        mock_client = Mock()
        mock_get_client.return_value = mock_client
        
        store = RedisOnlineStore()
        
        # Test with vector_enabled=False
        repo_config.online_store.vector_enabled = False
        with pytest.raises(ValueError, match="Vector search is not enabled"):
            store.retrieve_online_documents_v2(
                config=repo_config,
                table=vector_feature_view,
                requested_features=["vector"],
                embedding=[1.0, 2.0, 3.0],
                top_k=5
            )

    def test_distance_metric_mapping(self):
        """Test distance metric mapping."""
        from feast.infra.online_stores.redis import REDIS_DISTANCE_METRICS
        
        assert REDIS_DISTANCE_METRICS["cosine"] == "COSINE"
        assert REDIS_DISTANCE_METRICS["l2"] == "L2"
        assert REDIS_DISTANCE_METRICS["ip"] == "IP"
        assert REDIS_DISTANCE_METRICS["inner_product"] == "IP"
        assert REDIS_DISTANCE_METRICS["euclidean"] == "L2"
