import asyncio
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import List, Tuple, Union
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from feast import Entity, FeatureView, Field, FileSource, RepoConfig
from feast.errors import FeastExtrasDependencyImportError
from feast.infra.online_stores.helpers import _mmh3
from feast.infra.online_stores.redis import (
    RedisClient,
    RedisOnlineStore,
    RedisOnlineStoreConfig,
    RedisType,
    _glide_client_config,
    _glide_hmget_batch,
    _load_glide_sync,
)
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


def _dedup_config():
    return RepoConfig(
        provider="local",
        project="test",
        entity_key_serialization_version=3,
        registry="dummy_registry.db",
        online_store=RedisOnlineStoreConfig(),  # default: skip_dedup=False
    )


def _single_entity_batch(timestamps_and_values):
    """Build a write batch for one entity key, one row per (timestamp, value) pair."""
    entity_key = EntityKeyProto(
        join_keys=["entity"], entity_values=[ValueProto(int32_val=1)]
    )
    return [
        (entity_key, {"feature_10": ValueProto(int32_val=value)}, timestamp, None)
        for timestamp, value in timestamps_and_values
    ]


def _written_feature_values(hset_calls, feature_view_name, feature_name):
    """Extract the serialized feature value from each queued hset call."""
    f_key = _mmh3(f"{feature_view_name}:{feature_name}")
    return [call.kwargs["mapping"][f_key] for call in hset_calls]


@pytest.mark.parametrize(
    "order",
    ["descending", "ascending", "unordered"],
    ids=["descending", "ascending", "unordered"],
)
def test_online_write_batch_keeps_latest_event_time_within_batch(
    redis_online_store: RedisOnlineStore, feature_view, order
):
    """A batch containing several rows for one entity key must leave the value
    belonging to the latest event timestamp in the store, whatever order the rows
    arrive in.

    Regression test for #5163: previous timestamps were read once up front, so rows
    sharing an entity key all compared against the same pre-batch snapshot and could
    not see each other. Every row passed the staleness guard and the last row queued
    won, so a reverse-chronological batch stored the *oldest* value.
    """
    t1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    t2 = t1 + timedelta(seconds=5)
    t3 = t2 + timedelta(seconds=5)

    rows = {
        "descending": [(t3, 30), (t2, 20), (t1, 10)],
        "ascending": [(t1, 10), (t2, 20), (t3, 30)],
        "unordered": [(t2, 20), (t3, 30), (t1, 10)],
    }[order]
    data = _single_entity_batch(rows)

    mock_client = MagicMock()
    pipe = MagicMock()
    pipe.__enter__ = MagicMock(return_value=pipe)
    pipe.__exit__ = MagicMock(return_value=False)
    # No pre-existing value: one hmget result per row, each holding None.
    pipe.execute.side_effect = [[[None]] * len(data), []]
    mock_client.pipeline.return_value = pipe

    with patch.object(redis_online_store, "_get_client", return_value=mock_client):
        redis_online_store.online_write_batch(
            _dedup_config(), feature_view, data, progress=None
        )

    written = _written_feature_values(
        pipe.hset.call_args_list, feature_view.name, "feature_10"
    )
    assert written, "no write was queued for the batch"
    # Redis applies queued commands in order, so the surviving value is the last one.
    assert written[-1] == ValueProto(int32_val=30).SerializeToString()


def test_online_write_batch_async_keeps_latest_event_time_within_batch(
    redis_online_store: RedisOnlineStore, feature_view
):
    """online_write_batch_async must honour the same intra-batch ordering guarantee
    as the sync path (#5163)."""
    t1 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    t2 = t1 + timedelta(seconds=5)
    t3 = t2 + timedelta(seconds=5)
    data = _single_entity_batch([(t3, 30), (t2, 20), (t1, 10)])

    async_pipe = AsyncMock()
    async_pipe.__aenter__ = AsyncMock(return_value=async_pipe)
    async_pipe.__aexit__ = AsyncMock(return_value=False)
    async_pipe.execute = AsyncMock(side_effect=[[[None]] * len(data), []])
    async_pipe.hset = MagicMock()

    mock_async_client = AsyncMock()
    mock_async_client.pipeline = MagicMock(return_value=async_pipe)

    async def _run():
        with patch.object(
            redis_online_store,
            "_get_client_async",
            AsyncMock(return_value=mock_async_client),
        ):
            await redis_online_store.online_write_batch_async(
                _dedup_config(), feature_view, data, progress=None
            )

    asyncio.run(_run())

    written = _written_feature_values(
        async_pipe.hset.call_args_list, feature_view.name, "feature_10"
    )
    assert written, "no write was queued for the batch"
    assert written[-1] == ValueProto(int32_val=30).SerializeToString()


def test_online_write_batch_still_skips_rows_older_than_stored_value(
    redis_online_store: RedisOnlineStore, feature_view
):
    """The pre-existing staleness guard must keep working: a row older than the value
    already in Redis is dropped rather than written."""
    stored_ts = Timestamp()
    stored_ts.FromDatetime(datetime(2024, 1, 1, 12, 0, 10, tzinfo=timezone.utc))
    older = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    data = _single_entity_batch([(older, 10)])

    mock_client = MagicMock()
    pipe = MagicMock()
    pipe.__enter__ = MagicMock(return_value=pipe)
    pipe.__exit__ = MagicMock(return_value=False)
    pipe.execute.side_effect = [[[stored_ts.SerializeToString()]], []]
    mock_client.pipeline.return_value = pipe

    with patch.object(redis_online_store, "_get_client", return_value=mock_client):
        redis_online_store.online_write_batch(
            _dedup_config(), feature_view, data, progress=None
        )

    pipe.hset.assert_not_called()


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

    asyncio.run(_run())

    assert mock_async_client.pipeline.call_count == 1
    async_pipe.hmget.assert_not_called()
    async_pipe.hset.assert_called_once()


def test_online_write_batch_async_exists_and_is_coroutine():
    """online_write_batch_async must exist and be an async method (not raise NotImplementedError)."""
    import inspect

    store = RedisOnlineStore()
    assert hasattr(store, "online_write_batch_async")
    assert inspect.iscoroutinefunction(store.online_write_batch_async)


# ---------------------------------------------------------------------------------
# GLIDE client (client: glide)
# ---------------------------------------------------------------------------------

GLIDE_PATH = "feast.infra.online_stores.redis._load_glide_sync"
GLIDE_BATCH_PATH = "feast.infra.online_stores.redis._glide_hmget_batch"


def _online_read_config(client: RedisClient = RedisClient.redis) -> RepoConfig:
    return RepoConfig(
        provider="local",
        project="test",
        entity_key_serialization_version=3,
        registry="dummy_registry.db",
        online_store=RedisOnlineStoreConfig(client=client),
    )


def _pipeline_mock(results):
    pipe = MagicMock()
    pipe.__enter__ = MagicMock(return_value=pipe)
    pipe.__exit__ = MagicMock(return_value=False)
    pipe.execute.return_value = results
    return pipe


def _two_entity_keys() -> list:
    return [
        EntityKeyProto(join_keys=["entity"], entity_values=[ValueProto(int32_val=1)]),
        EntityKeyProto(join_keys=["entity"], entity_values=[ValueProto(int32_val=2)]),
    ]


def test_client_defaults_to_redis_py():
    """Default config must keep using redis-py."""
    assert RedisOnlineStoreConfig().client is RedisClient.redis


def test_default_client_reads_through_redis_pipeline(
    redis_online_store: RedisOnlineStore, feature_view
):
    """Without an opt-in, reads must go through the redis-py pipeline only."""
    pipe = _pipeline_mock([[None] * 4, [None] * 4])
    mock_client = MagicMock()
    mock_client.pipeline.return_value = pipe

    def fail_if_glide_is_loaded():
        raise AssertionError("glide_sync must not be loaded for the default client")

    with (
        patch.object(redis_online_store, "_get_client", return_value=mock_client),
        patch(GLIDE_PATH, side_effect=fail_if_glide_is_loaded),
    ):
        redis_online_store.online_read(
            _online_read_config(), feature_view, _two_entity_keys(), ["feature_10"]
        )

    assert mock_client.pipeline.call_count == 1
    assert pipe.hmget.call_count == 2


def test_glide_client_is_opt_in_per_config(
    redis_online_store: RedisOnlineStore, feature_view
):
    """client: glide must select the GLIDE batch and skip the redis-py pipeline."""
    pipe = _pipeline_mock([[None] * 4, [None] * 4])
    mock_client = MagicMock()
    mock_client.pipeline.return_value = pipe
    fake_glide_batch = MagicMock(return_value=[[None] * 4, [None] * 4])

    with (
        patch.object(redis_online_store, "_get_client", return_value=mock_client),
        patch(GLIDE_PATH, return_value=object()) as load_glide,
        patch.object(redis_online_store, "_get_glide_client", return_value=object()),
        patch(GLIDE_BATCH_PATH, fake_glide_batch),
    ):
        rows = redis_online_store.online_read(
            _online_read_config(RedisClient.glide),
            feature_view,
            _two_entity_keys(),
            ["feature_10"],
        )

    load_glide.assert_called_once()
    glide_commands = fake_glide_batch.call_args.args[2]
    assert len(glide_commands) == 2
    assert mock_client.pipeline.call_count == 0
    # Feature values are still converted with the shared redis-py response handling.
    assert len(rows) == 2


def test_glide_path_issues_same_commands_as_redis_path(
    redis_online_store: RedisOnlineStore, feature_view
):
    """Both clients must read the same entity keys with the same hashed fields."""
    entity_keys = _two_entity_keys()

    pipe = _pipeline_mock([[None] * 4, [None] * 4])
    mock_client = MagicMock()
    mock_client.pipeline.return_value = pipe
    with patch.object(redis_online_store, "_get_client", return_value=mock_client):
        redis_online_store.online_read(
            _online_read_config(), feature_view, entity_keys, ["feature_10"]
        )
    redis_commands = [call.args for call in pipe.hmget.call_args_list]
    fake_glide_batch = MagicMock(return_value=[[None] * 4, [None] * 4])

    with (
        patch(GLIDE_PATH, return_value=object()),
        patch.object(redis_online_store, "_get_glide_client", return_value=object()),
        patch(GLIDE_BATCH_PATH, fake_glide_batch),
    ):
        redis_online_store.online_read(
            _online_read_config(RedisClient.glide),
            feature_view,
            entity_keys,
            ["feature_10"],
        )

    glide_commands = fake_glide_batch.call_args.args[2]
    assert glide_commands == redis_commands
    # The hashed field names and the _ts field come from the existing helpers.
    key, fields = glide_commands[0]
    assert fields[0] == _mmh3(f"{feature_view.name}:feature_10")
    assert fields[-1] == f"_ts:{feature_view.name}"


def test_glide_hmget_batch_is_one_non_atomic_batch():
    """The GLIDE read must be a single non-atomic batch of HMGET commands."""

    class FakeBatch:
        def __init__(self, is_atomic):
            self.is_atomic = is_atomic
            self.commands = []

        def hmget(self, key, fields):
            self.commands.append((key, list(fields)))
            return self

    class FakeGlideClient:
        def __init__(self):
            self.exec_calls = []

        def exec(self, batch, raise_on_error):
            self.exec_calls.append((batch, raise_on_error))
            return [[b"value", None], [None, None]]

    fake_glide_sync = SimpleNamespace(
        Batch=FakeBatch,
        ClusterBatch=FakeBatch,
        GlideClusterClient=type("FakeGlideClusterClient", (), {}),
    )
    glide_client = FakeGlideClient()
    commands: List[Tuple[bytes, List[Union[str, bytes]]]] = [
        (b"key1", ["f1", "_ts:fv"]),
        (b"key2", ["f1", "_ts:fv"]),
    ]

    result = _glide_hmget_batch(fake_glide_sync, glide_client, commands)

    assert len(glide_client.exec_calls) == 1
    batch, raise_on_error = glide_client.exec_calls[0]
    assert batch.is_atomic is False
    assert raise_on_error is True
    assert batch.commands == commands
    assert result == [[b"value", None], [None, None]]


def test_glide_hmget_batch_uses_cluster_batch_for_cluster_clients():
    """Redis Cluster reads must use ClusterBatch, not the standalone Batch."""

    class FakeBatch:
        def __init__(self, is_atomic):
            self.is_atomic = is_atomic

        def hmget(self, key, fields):
            return self

    class FakeClusterBatch(FakeBatch):
        pass

    class FakeGlideClusterClient:
        def __init__(self):
            self.batch_types = []

        def exec(self, batch, raise_on_error):
            self.batch_types.append(type(batch))
            return [[None]]

    fake_glide_sync = SimpleNamespace(
        Batch=FakeBatch,
        ClusterBatch=FakeClusterBatch,
        GlideClusterClient=FakeGlideClusterClient,
    )
    glide_client = FakeGlideClusterClient()

    _glide_hmget_batch(fake_glide_sync, glide_client, [(b"key1", ["f1"])])

    assert glide_client.batch_types == [FakeClusterBatch]


def test_missing_glide_dependency_raises_clear_error(monkeypatch):
    """Opting in without the extra installed must fail with an actionable error."""
    monkeypatch.setitem(sys.modules, "glide_sync", None)

    with pytest.raises(FeastExtrasDependencyImportError) as exc_info:
        _load_glide_sync()

    assert "glide_sync" in str(exc_info.value)
    assert "pip install 'feast[glide]'" in str(exc_info.value)


def test_glide_client_config_parses_connection_string():
    glide_sync = pytest.importorskip("glide_sync")

    config = _glide_client_config(
        glide_sync,
        RedisOnlineStoreConfig(
            connection_string=(
                "redis.example.com:6380,db=2,password=hunter2,username=feast,"
                "ssl=true,socket_timeout=1.5,socket_connect_timeout=2"
            )
        ),
    )

    assert [(address.host, address.port) for address in config.addresses] == [
        ("redis.example.com", 6380)
    ]
    assert config.database_id == 2
    assert config.use_tls is True
    assert config.credentials.password == "hunter2"
    assert config.credentials.username == "feast"
    # redis-py takes timeouts in seconds, GLIDE in milliseconds.
    assert config.request_timeout == 1500
    assert config.advanced_config.connection_timeout == 2000


def test_glide_client_config_defaults_tls_and_timeouts_off():
    glide_sync = pytest.importorskip("glide_sync")

    config = _glide_client_config(
        glide_sync, RedisOnlineStoreConfig(connection_string="localhost:6379")
    )

    assert config.use_tls is False
    assert config.credentials is None
    assert config.database_id is None
    assert config.request_timeout is None
    assert config.advanced_config is None


def test_glide_client_config_parses_cluster_connection_string():
    glide_sync = pytest.importorskip("glide_sync")

    config = _glide_client_config(
        glide_sync,
        RedisOnlineStoreConfig(
            redis_type=RedisType.redis_cluster,
            connection_string="redis1:6379,redis2:6379,ssl=true,password=hunter2",
        ),
    )

    assert [(address.host, address.port) for address in config.addresses] == [
        ("redis1", 6379),
        ("redis2", 6379),
    ]
    assert config.use_tls is True
    assert config.credentials.password == "hunter2"
    # Cluster nodes do not support SELECT, so db is not forwarded.
    assert config.database_id is None


def test_glide_client_config_rejects_sentinel():
    """GLIDE has no Sentinel support, so the combination must fail loudly."""
    glide_sync = pytest.importorskip("glide_sync")

    with pytest.raises(ValueError, match="redis_sentinel"):
        _glide_client_config(
            glide_sync,
            RedisOnlineStoreConfig(
                redis_type=RedisType.redis_sentinel,
                connection_string="sentinel1:26379",
            ),
        )


def test_glide_client_config_warns_about_ignored_params(caplog):
    glide_sync = pytest.importorskip("glide_sync")

    with caplog.at_level("WARNING"):
        _glide_client_config(
            glide_sync,
            RedisOnlineStoreConfig(
                connection_string="localhost:6379,skip_full_coverage_check=true"
            ),
        )

    assert "skip_full_coverage_check" in caplog.text
