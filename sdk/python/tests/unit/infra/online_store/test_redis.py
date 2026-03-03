import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from feast import Entity, FeatureView, Field, FileSource, RepoConfig
from feast.infra.online_stores.redis import RedisOnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Int32


@pytest.fixture
def redis_online_store() -> RedisOnlineStore:
    return RedisOnlineStore()


@pytest.fixture
def repo_config():
    return RepoConfig(
        provider="local",
        project="test",
        entity_key_serialization_version=3,
        registry="dummy_registry.db",
    )


@pytest.fixture
def feature_view():
    file_source = FileSource(name="my_file_source", path="test.parquet")
    entity = Entity(name="entity", join_keys=["entity"])
    feature_view = FeatureView(
        name="feature_view_1",
        entities=[entity],
        schema=[
            Field(name="feature_10", dtype=Int32),
            Field(name="feature_11", dtype=Int32),
            Field(name="feature_12", dtype=Int32),
        ],
        source=file_source,
    )
    return feature_view


def test_generate_entity_redis_keys(redis_online_store: RedisOnlineStore, repo_config):
    entity_keys = [
        EntityKeyProto(join_keys=["entity"], entity_values=[ValueProto(int32_val=1)]),
    ]

    actual = redis_online_store._generate_redis_keys_for_entities(
        repo_config, entity_keys
    )
    expected = [
        b"\x01\x00\x00\x00\x02\x00\x00\x00\x06\x00\x00\x00entity\x03\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00test"
    ]
    assert actual == expected


def test_generate_hset_keys_for_features(
    redis_online_store: RedisOnlineStore, feature_view
):
    actual = redis_online_store._generate_hset_keys_for_features(feature_view)
    expected = (
        ["feature_10", "feature_11", "feature_12", "_ts:feature_view_1"],
        [b"&m_9", b"\xc37\x9a\xbf", b"wr\xb5d", "_ts:feature_view_1"],
    )
    assert actual == expected


def test_generate_hset_keys_for_features_with_requested_features(
    redis_online_store: RedisOnlineStore, feature_view
):
    actual = redis_online_store._generate_hset_keys_for_features(
        feature_view=feature_view, requested_features=["my-feature-view:feature1"]
    )
    expected = (
        ["my-feature-view:feature1", "_ts:feature_view_1"],
        [b"Si\x86J", "_ts:feature_view_1"],
    )
    assert actual == expected


def test_convert_redis_values_to_protobuf(
    redis_online_store: RedisOnlineStore, feature_view
):
    requested_features = [
        "feature_view_1:feature_10",
        "feature_view_1:feature_11",
        "_ts:feature_view_1",
    ]
    values = [
        [
            ValueProto(int32_val=1).SerializeToString(),
            ValueProto(int32_val=2).SerializeToString(),
            Timestamp().SerializeToString(),
        ]
    ]

    features = redis_online_store._convert_redis_values_to_protobuf(
        redis_values=values,
        feature_view=feature_view.name,
        requested_features=requested_features,
    )
    assert isinstance(features, list)
    assert len(features) == 1

    timestamp, features = features[0]
    assert features["feature_view_1:feature_10"].int32_val == 1
    assert features["feature_view_1:feature_11"].int32_val == 2


def test_get_features_for_entity(redis_online_store: RedisOnlineStore, feature_view):
    requested_features = [
        "feature_view_1:feature_10",
        "feature_view_1:feature_11",
        "_ts:feature_view_1",
    ]
    values = [
        ValueProto(int32_val=1).SerializeToString(),
        ValueProto(int32_val=2).SerializeToString(),
        Timestamp().SerializeToString(),
    ]

    timestamp, features = redis_online_store._get_features_for_entity(
        values=values,
        feature_view=feature_view.name,
        requested_features=requested_features,
    )
    assert "feature_view_1:feature_10" in features
    assert "feature_view_1:feature_11" in features
    assert features["feature_view_1:feature_10"].int32_val == 1
    assert features["feature_view_1:feature_11"].int32_val == 2


def test_get_features_for_entity_with_memoryview(
    redis_online_store: RedisOnlineStore, feature_view
):
    """Test that _get_features_for_entity handles memoryview inputs correctly.

    Redis may return memoryview objects instead of bytes in some cases.
    The optimized code should handle both without unnecessary conversions.
    """
    requested_features = [
        "feature_view_1:feature_10",
        "feature_view_1:feature_11",
        "_ts:feature_view_1",
    ]
    # Create memoryview objects to simulate redis returning memoryview
    val1_bytes = ValueProto(int32_val=100).SerializeToString()
    val2_bytes = ValueProto(int32_val=200).SerializeToString()
    ts_bytes = Timestamp(seconds=1234567890, nanos=123456789).SerializeToString()

    values = [
        memoryview(val1_bytes),
        memoryview(val2_bytes),
        memoryview(ts_bytes),
    ]

    timestamp, features = redis_online_store._get_features_for_entity(
        values=values,
        feature_view=feature_view.name,
        requested_features=requested_features,
    )
    assert features["feature_view_1:feature_10"].int32_val == 100
    assert features["feature_view_1:feature_11"].int32_val == 200
    assert timestamp is not None


def test_get_features_for_entity_with_none_values(
    redis_online_store: RedisOnlineStore, feature_view
):
    """Test that _get_features_for_entity handles None values correctly."""
    requested_features = [
        "feature_view_1:feature_10",
        "feature_view_1:feature_11",
        "_ts:feature_view_1",
    ]
    values = [
        ValueProto(int32_val=1).SerializeToString(),
        None,  # Missing feature value
        Timestamp().SerializeToString(),
    ]

    timestamp, features = redis_online_store._get_features_for_entity(
        values=values,
        feature_view=feature_view.name,
        requested_features=requested_features,
    )
    assert features["feature_view_1:feature_10"].int32_val == 1
    # None value should result in empty ValueProto
    assert features["feature_view_1:feature_11"].WhichOneof("val") is None


def test_convert_redis_values_to_protobuf_multiple_entities(
    redis_online_store: RedisOnlineStore, feature_view
):
    """Test batch conversion with multiple entities."""
    requested_features = [
        "feature_view_1:feature_10",
        "feature_view_1:feature_11",
        "_ts:feature_view_1",
    ]
    # Multiple entity values
    values = [
        [
            ValueProto(int32_val=1).SerializeToString(),
            ValueProto(int32_val=2).SerializeToString(),
            Timestamp(seconds=1000).SerializeToString(),
        ],
        [
            ValueProto(int32_val=10).SerializeToString(),
            ValueProto(int32_val=20).SerializeToString(),
            Timestamp(seconds=2000).SerializeToString(),
        ],
        [
            ValueProto(int32_val=100).SerializeToString(),
            ValueProto(int32_val=200).SerializeToString(),
            Timestamp(seconds=3000).SerializeToString(),
        ],
    ]

    results = redis_online_store._convert_redis_values_to_protobuf(
        redis_values=values,
        feature_view=feature_view.name,
        requested_features=requested_features,
    )

    assert len(results) == 3
    assert results[0][1]["feature_view_1:feature_10"].int32_val == 1
    assert results[1][1]["feature_view_1:feature_10"].int32_val == 10
    assert results[2][1]["feature_view_1:feature_10"].int32_val == 100


def test_get_features_for_entity_with_all_none_values(
    redis_online_store: RedisOnlineStore, feature_view
):
    """Test that None feature values result in empty ValueProto objects."""
    requested_features = [
        "feature_view_1:feature_10",
        "_ts:feature_view_1",
    ]
    # All None values except timestamp
    values = [
        None,
        Timestamp().SerializeToString(),
    ]

    timestamp, features = redis_online_store._get_features_for_entity(
        values=values,
        feature_view=feature_view.name,
        requested_features=requested_features,
    )
    # Even with None value, an empty ValueProto is created
    assert features is not None
    assert "feature_view_1:feature_10" in features
    assert features["feature_view_1:feature_10"].WhichOneof("val") is None
