from datetime import datetime
from unittest.mock import patch

import pytest

from feast import Entity, FeatureView, Field, FileSource, RepoConfig, ValueType
from feast.infra.online_stores.hybrid_online_store.hybrid_online_store import (
    HybridOnlineStore,
    HybridOnlineStoreConfig,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey
from feast.protos.feast.types.Value_pb2 import Value
from feast.types import PrimitiveFeastType


@pytest.fixture
def sample_entity():
    return Entity(name="id", join_keys=["id"], value_type=ValueType.INT64)


@pytest.fixture
def sample_feature_view(sample_entity):
    file_source = FileSource(
        path="/tmp/feast_hybrid_test.parquet",
        event_timestamp_column="event_timestamp",
    )
    return FeatureView(
        name="test_fv",
        entities=[sample_entity],
        schema=[Field(name="feature1", dtype=PrimitiveFeastType.INT64)],
        online=True,
        tags={"tribe": "redis"},
        source=file_source,
    )


@pytest.fixture
def sample_repo_config():
    # Minimal config for HybridOnlineStore with two backends (mocked for test)
    return RepoConfig(
        registry="test-registry.db",
        project="test_project",
        provider="local",
        online_store=HybridOnlineStoreConfig(
            online_stores=[
                HybridOnlineStoreConfig.OnlineStoresWithConfig(
                    type="redis",
                    conf={"redis_type": "redis", "connection_string": "localhost:6379"},
                ),
                HybridOnlineStoreConfig.OnlineStoresWithConfig(
                    type="sqlite",
                    conf={"path": "/tmp/feast_hybrid_test.db"},
                ),
            ]
        ),
        offline_store=None,
    )


@pytest.mark.usefixtures("sample_entity", "sample_feature_view", "sample_repo_config")
def test_hybrid_online_store_write_and_read(sample_repo_config, sample_feature_view):
    with (
        patch(
            "feast.infra.online_stores.redis.RedisOnlineStore.online_write_batch"
        ) as mock_write,
        patch(
            "feast.infra.online_stores.redis.RedisOnlineStore.online_read"
        ) as mock_read,
    ):
        mock_write.return_value = None
        mock_read.return_value = [(None, {"feature1": Value(int64_val=100)})]
        store = HybridOnlineStore()
        entity_key = EntityKey(
            join_keys=["id"],
            entity_values=[Value(int64_val=1)],
        )
        now = datetime.utcnow()
        odata = [(entity_key, {"feature1": Value(int64_val=100)}, now, None)]
        # Write to the online store (mocked)
        store.online_write_batch(
            sample_repo_config, sample_feature_view, odata, progress=None
        )
        # Read back (mocked)
        result = store.online_read(
            sample_repo_config, sample_feature_view, [entity_key]
        )
        assert result[0][1]["feature1"].int64_val == 100
