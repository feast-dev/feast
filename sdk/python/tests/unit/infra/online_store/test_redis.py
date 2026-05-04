import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from feast import Entity, FeatureView, Field, FileSource, RepoConfig
from feast.infra.online_stores.redis import RedisOnlineStore, RedisOnlineStoreConfig
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


def _make_pipe_mock(hkeys_results):
    """Return a MagicMock pipeline whose execute() yields hkeys_results then does nothing."""
    pipe = MagicMock()
    pipe.__enter__ = MagicMock(return_value=pipe)
    pipe.__exit__ = MagicMock(return_value=False)
    pipe.execute = MagicMock(side_effect=[hkeys_results, None])
    return pipe


def test_delete_table_does_not_call_hgetall(
    redis_online_store: RedisOnlineStore, repo_config, feature_view
):
    """delete_table must not call hgetall directly (old N+1 pattern)."""
    fv_name = feature_view.name
    fv_bytes = fv_name.encode("utf8")

    mock_client = MagicMock()
    mock_client.scan_iter.return_value = iter([b"key1", b"key2"])

    pipe = _make_pipe_mock(
        [
            [b"_ts:" + fv_bytes],  # key1: only this FV → DEL
            [b"_ts:" + fv_bytes, b"_ts:other_fv"],  # key2: shared → HDEL
        ]
    )
    mock_client.pipeline.return_value = pipe

    with patch.object(redis_online_store, "_get_client", return_value=mock_client):
        redis_online_store.delete_table(repo_config, feature_view)

    mock_client.hgetall.assert_not_called()
    # Two pipeline context managers: one for hkeys, one for deletions
    assert mock_client.pipeline.call_count == 2
    # hkeys was queued for both keys
    assert pipe.hkeys.call_count == 2


def test_delete_table_skips_unrelated_keys(
    redis_online_store: RedisOnlineStore, repo_config, feature_view
):
    """delete_table must not issue delete/hdel for keys that don't have this FV."""
    mock_client = MagicMock()
    mock_client.scan_iter.return_value = iter([b"key1"])

    pipe = _make_pipe_mock(
        [
            [b"_ts:other_fv"],  # key1 belongs to a different FV → skip
        ]
    )
    mock_client.pipeline.return_value = pipe

    with patch.object(redis_online_store, "_get_client", return_value=mock_client):
        redis_online_store.delete_table(repo_config, feature_view)

    pipe.delete.assert_not_called()
    pipe.hdel.assert_not_called()


def test_delete_table_no_keys_skips_pipelines(
    redis_online_store: RedisOnlineStore, repo_config, feature_view
):
    """When scan finds no keys, no pipeline should be opened."""
    mock_client = MagicMock()
    mock_client.scan_iter.return_value = iter([])

    with patch.object(redis_online_store, "_get_client", return_value=mock_client):
        redis_online_store.delete_table(repo_config, feature_view)

    mock_client.pipeline.assert_not_called()


def test_skip_dedup_default_is_false():
    """skip_dedup must default to False for backward compatibility."""
    cfg = RedisOnlineStoreConfig()
    assert cfg.skip_dedup is False


def test_skip_dedup_can_be_enabled():
    """skip_dedup can be set to True via config."""
    cfg = RedisOnlineStoreConfig(skip_dedup=True)
    assert cfg.skip_dedup is True


def test_online_write_batch_skip_dedup_single_pipeline(
    redis_online_store: RedisOnlineStore, repo_config, feature_view
):
    """When skip_dedup=True, online_write_batch must use exactly 1 pipeline execution
    (no initial timestamp read pipeline)."""
    online_store_cfg = RedisOnlineStoreConfig(skip_dedup=True)
    config = RepoConfig(
        provider="local",
        project="test",
        entity_key_serialization_version=3,
        registry="dummy_registry.db",
        online_store=online_store_cfg,
    )

    mock_client = MagicMock()
    pipe = MagicMock()
    pipe.__enter__ = MagicMock(return_value=pipe)
    pipe.__exit__ = MagicMock(return_value=False)
    pipe.execute.return_value = []
    mock_client.pipeline.return_value = pipe

    data = [
        (
            EntityKeyProto(
                join_keys=["entity"], entity_values=[ValueProto(int32_val=1)]
            ),
            {"feature_10": ValueProto(int32_val=100)},
            datetime.now(tz=timezone.utc),
            None,
        )
    ]

    with patch.object(redis_online_store, "_get_client", return_value=mock_client):
        redis_online_store.online_write_batch(config, feature_view, data, progress=None)

    # Only 1 pipeline context opened (no read pipeline for timestamps)
    assert mock_client.pipeline.call_count == 1
    # No hmget (timestamp reads) issued
    pipe.hmget.assert_not_called()
    # hset was called to write the data
    pipe.hset.assert_called_once()


def test_online_write_batch_with_dedup_uses_two_pipelines(
    redis_online_store: RedisOnlineStore, feature_view
):
    """When skip_dedup=False (default), online_write_batch reads timestamps first
    then writes in the same pipeline context (hmget + hset in one `with` block)."""
    config = RepoConfig(
        provider="local",
        project="test",
        entity_key_serialization_version=3,
        registry="dummy_registry.db",
        online_store=RedisOnlineStoreConfig(),  # default: skip_dedup=False
    )

    mock_client = MagicMock()
    pipe = MagicMock()
    pipe.__enter__ = MagicMock(return_value=pipe)
    pipe.__exit__ = MagicMock(return_value=False)
    # hmget returns a list per field queried; execute() returns one list per pipeline command.
    # For one entity querying one ts_key: [[None]] (one hmget result, value is None)
    pipe.execute.side_effect = [[[None]], []]
    mock_client.pipeline.return_value = pipe

    data = [
        (
            EntityKeyProto(
                join_keys=["entity"], entity_values=[ValueProto(int32_val=1)]
            ),
            {"feature_10": ValueProto(int32_val=100)},
            datetime.now(tz=timezone.utc),
            None,
        )
    ]

    with patch.object(redis_online_store, "_get_client", return_value=mock_client):
        redis_online_store.online_write_batch(config, feature_view, data, progress=None)

    # pipeline context opened once (both read and write phases use the same `with` block)
    assert mock_client.pipeline.call_count == 1
    # hmget was issued for the timestamp check
    pipe.hmget.assert_called_once()


def test_online_write_batch_async_skip_dedup_single_pipeline(
    redis_online_store: RedisOnlineStore, feature_view
):
    """online_write_batch_async with skip_dedup=True must use exactly 1 pipeline."""
    online_store_cfg = RedisOnlineStoreConfig(skip_dedup=True)
    config = RepoConfig(
        provider="local",
        project="test",
        entity_key_serialization_version=3,
        registry="dummy_registry.db",
        online_store=online_store_cfg,
    )

    async_pipe = AsyncMock()
    async_pipe.__aenter__ = AsyncMock(return_value=async_pipe)
    async_pipe.__aexit__ = AsyncMock(return_value=False)
    async_pipe.execute = AsyncMock(return_value=[])

    mock_async_client = AsyncMock()
    mock_async_client.pipeline = MagicMock(return_value=async_pipe)

    data = [
        (
            EntityKeyProto(
                join_keys=["entity"], entity_values=[ValueProto(int32_val=1)]
            ),
            {"feature_10": ValueProto(int32_val=100)},
            datetime.now(tz=timezone.utc),
            None,
        )
    ]

    async def _run():
        with patch.object(
            redis_online_store,
            "_get_client_async",
            AsyncMock(return_value=mock_async_client),
        ):
            await redis_online_store.online_write_batch_async(
                config, feature_view, data, progress=None
            )

    asyncio.get_event_loop().run_until_complete(_run())

    assert mock_async_client.pipeline.call_count == 1
    async_pipe.hmget.assert_not_called()
    async_pipe.hset.assert_called_once()


def test_online_write_batch_async_exists_and_is_coroutine():
    """online_write_batch_async must exist and be an async method (not raise NotImplementedError)."""
    import inspect

    store = RedisOnlineStore()
    assert hasattr(store, "online_write_batch_async")
    assert inspect.iscoroutinefunction(store.online_write_batch_async)
