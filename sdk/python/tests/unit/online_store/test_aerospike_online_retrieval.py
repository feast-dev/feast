"""
Unit tests for the Aerospike online store.

Most of the tests here are pure Python and run in any environment (they cover
the timestamp/TTL helpers, the column-oriented proto reshape, and the
write/read/admin dispatch with a mocked Aerospike client). One end-to-end test
is marked with ``@_requires_docker`` and is skipped when Docker is unavailable.
"""

import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("aerospike")

import aerospike  # noqa: E402

from feast import FeatureView, Field, FileSource  # noqa: E402
from feast.infra.online_stores.aerospike_online_store.aerospike import (  # noqa: E402
    AerospikeOnlineStore,
    _datetime_to_epoch_ms,
    _epoch_ms_to_datetime,
    _resolve_ttl,
)
from feast.protos.feast.types.EntityKey_pb2 import (
    EntityKey as EntityKeyProto,  # noqa: E402
)
from feast.protos.feast.types.Value_pb2 import Value as ValueProto  # noqa: E402
from feast.repo_config import RepoConfig  # noqa: E402
from feast.types import Float64, Int64  # noqa: E402
from feast.utils import _utc_now  # noqa: E402
from tests.universal.feature_repos.universal.online_store.aerospike import (  # noqa: E402
    AEROSPIKE_CE_IMAGE,
)
from tests.utils.cli_repo_creator import CliRunner, get_example_repo  # noqa: E402

docker_available = False
try:
    import docker
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.waiting_utils import wait_for_logs

    try:
        _docker = docker.from_env()
        _docker.ping()
        docker_available = True
    except Exception:
        pass
except ImportError:
    pass

_requires_docker = pytest.mark.skipif(
    not docker_available,
    reason="Docker is not available or not running. Start Docker daemon to run these tests.",
)


# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------


def _make_fv(*field_names: str, dtype=Int64) -> FeatureView:
    """Build a minimal FeatureView for conversion tests."""
    return FeatureView(
        name="test_fv",
        entities=[],
        schema=[Field(name=n, dtype=dtype) for n in field_names],
        source=FileSource(path="fake.parquet", timestamp_field="event_timestamp"),
        ttl=timedelta(days=1),
    )


def _aerospike_repo_config(**online_store_overrides) -> RepoConfig:
    base = {"type": "aerospike", "namespace": "feast"}
    base.update(online_store_overrides)
    return RepoConfig(
        project="demo",
        provider="local",
        registry="/tmp/reg.db",
        online_store=base,
        entity_key_serialization_version=3,
    )


def _fake_batch_record(key: tuple, bins):
    """Mimic aerospike_helpers.batch.records.BatchRecord for a successful read."""
    return SimpleNamespace(
        key=key,
        result=0,
        record=(key, {"ttl": 0, "gen": 1}, bins) if bins is not None else None,
        in_doubt=False,
    )


# ---------------------------------------------------------------------------
# Helpers: timestamp and TTL conversions
# ---------------------------------------------------------------------------


def test_datetime_helpers_round_trip_utc():
    dt = datetime(2026, 4, 20, 12, 30, 45, 123000, tzinfo=timezone.utc)
    ms = _datetime_to_epoch_ms(dt)
    assert _epoch_ms_to_datetime(ms) == dt


def test_datetime_helpers_treat_naive_as_utc():
    dt_naive = datetime(2026, 4, 20, 12, 30, 45, 123000)
    dt_utc = dt_naive.replace(tzinfo=timezone.utc)
    assert _datetime_to_epoch_ms(dt_naive) == _datetime_to_epoch_ms(dt_utc)


def test_epoch_ms_to_datetime_none_passthrough():
    assert _epoch_ms_to_datetime(None) is None


def test_resolve_ttl_sentinels():
    assert _resolve_ttl(None) == aerospike.TTL_NAMESPACE_DEFAULT
    assert _resolve_ttl(0) == aerospike.TTL_NEVER_EXPIRE
    assert _resolve_ttl(3600) == 3600


def test_socket_timeout_ms_propagates_to_read_write_and_batch_policies(monkeypatch):
    """``socket_timeout_ms`` must apply uniformly to read, write and batch
    policies — otherwise ``max_retries`` can't fire within the total budget
    (the first attempt alone consumes the whole deadline)."""
    captured: dict = {}

    def fake_client(cfg):
        captured["cfg"] = cfg
        fake = MagicMock()
        fake.connect.return_value = fake
        return fake

    monkeypatch.setattr(aerospike, "client", fake_client)

    config = _aerospike_repo_config(
        batch_total_timeout_ms=1500,
        socket_timeout_ms=40,
        read_timeout_ms=100,
        write_timeout_ms=200,
    )
    store = AerospikeOnlineStore()
    store._get_client(config)

    policies = captured["cfg"]["policies"]
    assert policies["read"]["total_timeout"] == 100
    assert policies["read"]["socket_timeout"] == 40
    assert policies["write"]["total_timeout"] == 200
    assert policies["write"]["socket_timeout"] == 40
    assert policies["batch"]["total_timeout"] == 1500
    assert policies["batch"]["socket_timeout"] == 40


def test_socket_timeout_ms_omitted_when_not_set(monkeypatch):
    """Unset ``socket_timeout_ms`` must leave the client's default in place,
    not inject ``None`` into the policy dicts."""
    captured: dict = {}

    def fake_client(cfg):
        captured["cfg"] = cfg
        fake = MagicMock()
        fake.connect.return_value = fake
        return fake

    monkeypatch.setattr(aerospike, "client", fake_client)

    store = AerospikeOnlineStore()
    store._get_client(_aerospike_repo_config())

    policies = captured["cfg"]["policies"]
    for scope in ("read", "write", "batch"):
        assert "socket_timeout" not in policies[scope], (
            f"socket_timeout leaked into {scope} policy with no config override"
        )


# ---------------------------------------------------------------------------
# _convert_raw_docs_to_proto — same contract as MongoDB's helper
# ---------------------------------------------------------------------------


def test_convert_raw_docs_missing_entity():
    """Entity key absent from docs -> (None, None)."""
    fv = _make_fv("score")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ids = [b"present", b"missing"]
    docs = {
        b"present": {
            "features": {"test_fv": {"score": 42}},
            "event_timestamps": {"test_fv": ts},
        }
    }

    results = AerospikeOnlineStore._convert_raw_docs_to_proto(ids, docs, fv)

    assert len(results) == 2
    ts_out, feats_out = results[0]
    assert ts_out == ts
    assert feats_out["score"].int64_val == 42
    assert results[1] == (None, None)


def test_convert_raw_docs_partial_doc():
    """Entity exists but one feature key is absent -> empty ValueProto for that feature."""
    fv = _make_fv("present_feat", "missing_feat")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ids = [b"entity1"]
    docs = {
        b"entity1": {
            "features": {"test_fv": {"present_feat": 99}},
            "event_timestamps": {"test_fv": ts},
        }
    }

    results = AerospikeOnlineStore._convert_raw_docs_to_proto(ids, docs, fv)

    assert len(results) == 1
    ts_out, feats_out = results[0]
    assert ts_out == ts
    assert feats_out["present_feat"].int64_val == 99
    assert feats_out["missing_feat"] == ValueProto()


def test_convert_raw_docs_entity_exists_but_fv_not_written():
    """Entity doc exists (written by another FV) but this FV was never written -> (None, None)."""
    pricing_fv = _make_fv("price")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ids = [b"driver_1"]
    docs = {
        b"driver_1": {
            "features": {"driver_stats": {"acc_rate": 0.9}},
            "event_timestamps": {"driver_stats": ts},
        }
    }

    results = AerospikeOnlineStore._convert_raw_docs_to_proto(ids, docs, pricing_fv)

    assert len(results) == 1
    assert results[0] == (None, None)


def test_convert_raw_docs_ordering():
    """Result order matches the ids list regardless of dict insertion order in docs."""
    fv = _make_fv("score")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)

    ids = [b"entity_z", b"entity_a", b"entity_m"]
    docs = {
        b"entity_a": {
            "features": {"test_fv": {"score": 2}},
            "event_timestamps": {"test_fv": ts},
        },
        b"entity_m": {
            "features": {"test_fv": {"score": 3}},
            "event_timestamps": {"test_fv": ts},
        },
        b"entity_z": {
            "features": {"test_fv": {"score": 1}},
            "event_timestamps": {"test_fv": ts},
        },
    }

    results = AerospikeOnlineStore._convert_raw_docs_to_proto(ids, docs, fv)

    assert [row[1]["score"].int64_val for row in results] == [1, 2, 3]


# ---------------------------------------------------------------------------
# Write path: _build_batch_writes + online_write_batch dispatch
# ---------------------------------------------------------------------------


def _entity_key(join_key: str, value: int) -> EntityKeyProto:
    return EntityKeyProto(
        join_keys=[join_key], entity_values=[ValueProto(int64_val=value)]
    )


def test_build_batch_writes_produces_three_ops_with_created_ts():
    config = _aerospike_repo_config(ttl_seconds=3600)
    fv = SimpleNamespace(name="driver_stats")
    ts = datetime(2026, 4, 20, 12, 30, 45, tzinfo=timezone.utc)
    row = (
        _entity_key("driver_id", 1),
        {
            "rating": ValueProto(double_val=4.91),
            "trips_last_7d": ValueProto(int64_val=132),
        },
        ts,
        ts,
    )

    batch = AerospikeOnlineStore._build_batch_writes(
        config, fv, [row], namespace="feast", set_name="demo_latest"
    )

    assert len(batch.batch_records) == 1
    bw = batch.batch_records[0]
    assert bw.key[:2] == ("feast", "demo_latest")
    # Must be bytearray, not bytes: the Aerospike Python C client rejects
    # bytes user keys ("Key is invalid") and silently hashes only the first
    # byte inside batch_operate/batch_read — causing digest collisions.
    assert isinstance(bw.key[2], bytearray)
    assert bw.meta == {"ttl": 3600}
    # No explicit per-record policy: writes rely on the client-level default
    # (POLICY_KEY_DIGEST), which doesn't persist the serialized entity key
    # server-side — batch_operate preserves request order so the stored key
    # has no functional use on the read path.
    assert bw.policy is None
    assert len(bw.ops) == 3
    bin_names = [op["bin"] for op in bw.ops]
    assert bin_names == ["features", "event_ts", "created_ts"]
    # Map CDTs must be created with MAP_KEY_ORDERED so key lookups on reads
    # and the update() background scan stay O(log N).
    for op in bw.ops:
        if op["bin"] in ("features", "event_ts"):
            assert op["map_policy"] == {"map_order": aerospike.MAP_KEY_ORDERED}


def test_build_batch_writes_omits_created_ts_when_none():
    config = _aerospike_repo_config()
    fv = SimpleNamespace(name="driver_stats")
    row = (
        _entity_key("driver_id", 1),
        {"rating": ValueProto(double_val=4.91)},
        datetime(2026, 4, 20, tzinfo=timezone.utc),
        None,
    )

    batch = AerospikeOnlineStore._build_batch_writes(
        config, fv, [row], namespace="feast", set_name="demo_latest"
    )

    ops = batch.batch_records[0].ops
    assert len(ops) == 2
    assert {op["bin"] for op in ops} == {"features", "event_ts"}


def test_build_batch_writes_ttl_sentinels():
    config = _aerospike_repo_config(ttl_seconds=0)
    fv = SimpleNamespace(name="fv")
    row = (
        _entity_key("id", 1),
        {"x": ValueProto(int64_val=1)},
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        None,
    )

    batch = AerospikeOnlineStore._build_batch_writes(
        config, fv, [row], namespace="feast", set_name="set"
    )
    assert batch.batch_records[0].meta == {"ttl": aerospike.TTL_NEVER_EXPIRE}


def test_online_write_batch_dispatches_to_client():
    config = _aerospike_repo_config()
    fv = SimpleNamespace(name="fv")
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    store._client = fake_client

    row = (
        _entity_key("id", 1),
        {"x": ValueProto(int64_val=1)},
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        None,
    )
    progress_calls: list[int] = []
    store.online_write_batch(config, fv, [row], progress=progress_calls.append)

    assert fake_client.batch_write.called
    assert progress_calls == [1]


def test_online_write_batch_empty_short_circuits():
    config = _aerospike_repo_config()
    fv = SimpleNamespace(name="fv")
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    store._client = fake_client

    progress_calls: list[int] = []
    store.online_write_batch(config, fv, [], progress=progress_calls.append)

    assert not fake_client.batch_write.called
    assert progress_calls == [0]


def test_chunked_splits_without_partial_tail():
    chunks = list(AerospikeOnlineStore._chunked([1, 2, 3, 4, 5], 2))
    assert chunks == [[1, 2], [3, 4], [5]]


def test_get_client_thread_safe_single_connect(monkeypatch):
    """Concurrent first callers must share one client, not leak connections."""
    connect_count = 0
    connect_lock = threading.Lock()

    def fake_client(cfg):
        fake = MagicMock()

        def connect():
            nonlocal connect_count
            with connect_lock:
                connect_count += 1
            time.sleep(0.05)
            return fake

        fake.connect = connect
        return fake

    monkeypatch.setattr(aerospike, "client", fake_client)
    store = AerospikeOnlineStore()
    config = _aerospike_repo_config()
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        store._get_client(config)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert connect_count == 1


def test_online_write_batch_chunks_at_batch_max_records():
    config = _aerospike_repo_config(batch_max_records=100)
    fv = SimpleNamespace(name="fv")
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    store._client = fake_client

    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [
        (
            _entity_key("id", i),
            {"x": ValueProto(int64_val=i)},
            ts,
            None,
        )
        for i in range(250)
    ]
    progress_calls: list[int] = []
    store.online_write_batch(config, fv, rows, progress=progress_calls.append)

    assert fake_client.batch_write.call_count == 3
    sizes = [
        len(call.args[0].batch_records)
        for call in fake_client.batch_write.call_args_list
    ]
    assert sizes == [100, 100, 50]
    assert progress_calls == [100, 200, 250]


# ---------------------------------------------------------------------------
# Read path: online_read dispatches and converts via batch_operate
# ---------------------------------------------------------------------------


def _read_feature_view() -> SimpleNamespace:
    """Minimal FV object exposing .name and .features with dtype mappings."""
    return SimpleNamespace(
        name="driver_stats",
        features=[
            SimpleNamespace(name="rating", dtype=Float64),
            SimpleNamespace(name="trips_last_7d", dtype=Int64),
        ],
    )


def test_online_read_happy_path_with_projection_and_ordering():
    config = _aerospike_repo_config()
    fv = _read_feature_view()
    store = AerospikeOnlineStore()
    ts = datetime(2026, 4, 20, 12, 30, 45, 123000, tzinfo=timezone.utc)

    ek1 = _entity_key("driver_id", 1)
    ek2 = _entity_key("driver_id", 2)
    ek3 = _entity_key("driver_id", 3)
    key1 = store._aerospike_key(config, ek1)
    key2 = store._aerospike_key(config, ek2)
    key3 = store._aerospike_key(config, ek3)

    def fake_batch_operate(keys, ops):
        assert keys == [key1, key2, key3]
        assert len(ops) == 2
        assert ops[0]["bin"] == "features"
        assert ops[1]["bin"] == "event_ts"
        # Aerospike's batch_operate preserves input order; the middle record
        # is simulated as missing to verify we still emit (None, None) in
        # the correct slot.
        br1 = _fake_batch_record(
            key1,
            {
                "features": {"rating": 4.91, "trips_last_7d": 132},
                "event_ts": _datetime_to_epoch_ms(ts),
            },
        )
        br2 = _fake_batch_record(key2, None)  # missing record
        br3 = _fake_batch_record(
            key3,
            {
                "features": {"rating": 3.75, "trips_last_7d": 42},
                "event_ts": _datetime_to_epoch_ms(ts),
            },
        )
        return SimpleNamespace(batch_records=[br1, br2, br3])

    fake_client = MagicMock()
    fake_client.batch_operate.side_effect = fake_batch_operate
    store._client = fake_client

    results = store.online_read(config, fv, [ek1, ek2, ek3])

    assert len(results) == 3
    ts0, feats0 = results[0]
    assert ts0 == ts
    assert abs(feats0["rating"].double_val - 4.91) < 1e-9
    assert feats0["trips_last_7d"].int64_val == 132
    assert results[1] == (None, None)
    ts2, feats2 = results[2]
    assert ts2 == ts
    assert abs(feats2["rating"].double_val - 3.75) < 1e-9


def test_online_read_empty_keys_returns_empty():
    store = AerospikeOnlineStore()
    store._client = MagicMock()
    fv = _read_feature_view()
    assert store.online_read(_aerospike_repo_config(), fv, []) == []
    assert not store._client.batch_operate.called


def test_online_read_chunks_at_batch_max_records():
    config = _aerospike_repo_config(batch_max_records=100)
    fv = _read_feature_view()
    store = AerospikeOnlineStore()
    entity_keys = [_entity_key("driver_id", i) for i in range(250)]
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def fake_batch_operate(keys, ops):
        return SimpleNamespace(
            batch_records=[
                _fake_batch_record(
                    key,
                    {
                        "features": {"rating": 4.0, "trips_last_7d": 10},
                        "event_ts": _datetime_to_epoch_ms(ts),
                    },
                )
                for key in keys
            ]
        )

    fake_client = MagicMock()
    fake_client.batch_operate.side_effect = fake_batch_operate
    store._client = fake_client

    results = store.online_read(config, fv, entity_keys)

    assert fake_client.batch_operate.call_count == 3
    sizes = [len(call.args[0]) for call in fake_client.batch_operate.call_args_list]
    assert sizes == [100, 100, 50]
    assert len(results) == 250
    assert all(feats is not None for _, feats in results)


def test_online_read_record_exists_but_fv_not_present_returns_none():
    config = _aerospike_repo_config()
    fv = _read_feature_view()
    store = AerospikeOnlineStore()
    ek = _entity_key("driver_id", 1)
    key = store._aerospike_key(config, ek)

    def fake_batch_operate(keys, ops):
        return SimpleNamespace(
            batch_records=[
                _fake_batch_record(key, {"features": None, "event_ts": None})
            ]
        )

    fake_client = MagicMock()
    fake_client.batch_operate.side_effect = fake_batch_operate
    store._client = fake_client

    results = store.online_read(config, fv, [ek])
    assert results == [(None, None)]


def _fake_error_record(key: tuple, result_code: int):
    """Batch response for an error case — no ``record`` but a non-OK result."""
    return SimpleNamespace(key=key, result=result_code, record=None, in_doubt=False)


def test_online_read_record_not_found_returns_none():
    """``br.result == 2`` (RECORD_NOT_FOUND) is a genuine miss, not an error."""
    config = _aerospike_repo_config()
    fv = _read_feature_view()
    store = AerospikeOnlineStore()
    ek = _entity_key("driver_id", 1)
    key = store._aerospike_key(config, ek)

    fake_client = MagicMock()
    fake_client.batch_operate.return_value = SimpleNamespace(
        batch_records=[_fake_error_record(key, result_code=2)]
    )
    store._client = fake_client

    assert store.online_read(config, fv, [ek]) == [(None, None)]


def test_online_read_op_not_applicable_returns_none():
    """``br.result == 26`` (OP_NOT_APPLICABLE) happens when the nested FV
    submap is absent — also a miss under the OnlineStore contract, not an
    error."""
    config = _aerospike_repo_config()
    fv = _read_feature_view()
    store = AerospikeOnlineStore()
    ek = _entity_key("driver_id", 1)
    key = store._aerospike_key(config, ek)

    fake_client = MagicMock()
    fake_client.batch_operate.return_value = SimpleNamespace(
        batch_records=[_fake_error_record(key, result_code=26)]
    )
    store._client = fake_client

    assert store.online_read(config, fv, [ek]) == [(None, None)]


def test_online_read_raises_on_transient_error():
    """Any other non-OK result (e.g. 9 = TIMEOUT) must be surfaced as an error.

    A silent null-return on a transient timeout is the root-cause class of
    bug that shows up weeks later as a model-quality regression; pinning
    this with a test keeps the read path loud on failure.
    """
    config = _aerospike_repo_config()
    fv = _read_feature_view()
    store = AerospikeOnlineStore()
    ek = _entity_key("driver_id", 1)
    key = store._aerospike_key(config, ek)

    fake_client = MagicMock()
    fake_client.batch_operate.return_value = SimpleNamespace(
        batch_records=[_fake_error_record(key, result_code=9)]  # TIMEOUT
    )
    store._client = fake_client

    with pytest.raises(RuntimeError, match="non-OK status"):
        store.online_read(config, fv, [ek])


def test_online_read_projects_requested_features_server_side():
    """``requested_features`` must drive a ``map_get_by_key_list`` projection
    nested into the feature-view submap via ``cdt_ctx``; the unordered
    ``MAP_RETURN_KEY_VALUE`` response is a flat ``[k1,v1,k2,v2]`` list."""
    config = _aerospike_repo_config()
    fv = _read_feature_view()
    store = AerospikeOnlineStore()
    ek = _entity_key("driver_id", 1)
    key = store._aerospike_key(config, ek)
    ts = datetime(2026, 4, 20, 12, 30, 45, tzinfo=timezone.utc)

    captured_ops: list = []

    def fake_batch_operate(keys, ops):
        captured_ops.extend(ops)
        # Projected features come back as a flat [k,v,k,v] list rather than
        # a dict — the store must normalize this before feeding the row
        # reshape helper.
        return SimpleNamespace(
            batch_records=[
                _fake_batch_record(
                    key,
                    {
                        "features": ["rating", 4.91],
                        "event_ts": _datetime_to_epoch_ms(ts),
                    },
                )
            ]
        )

    fake_client = MagicMock()
    fake_client.batch_operate.side_effect = fake_batch_operate
    store._client = fake_client

    results = store.online_read(config, fv, [ek], requested_features=["rating"])

    assert len(results) == 1
    ts_out, feats = results[0]
    assert ts_out == ts
    assert abs(feats["rating"].double_val - 4.91) < 1e-9
    assert feats["trips_last_7d"] == ValueProto()
    features_op = captured_ops[0]
    assert features_op["op"] == aerospike.OP_MAP_GET_BY_KEY_LIST
    assert features_op["bin"] == "features"
    assert features_op["val"] == ["rating"]
    assert features_op["return_type"] == aerospike.MAP_RETURN_KEY_VALUE
    # The ctx must nest the projection inside the FV's submap so the server
    # only ships the requested columns over the wire.
    assert features_op.get("ctx"), "projection op is missing ctx"


def test_normalize_projected_features_handles_all_payload_shapes():
    """The shape of the ``features`` payload depends on which op produced it;
    the helper must accept all of them."""
    assert AerospikeOnlineStore._normalize_projected_features(None) is None
    assert AerospikeOnlineStore._normalize_projected_features([]) == {}
    assert AerospikeOnlineStore._normalize_projected_features(["a", 1, "b", 2]) == {
        "a": 1,
        "b": 2,
    }
    assert AerospikeOnlineStore._normalize_projected_features({"a": 1, "b": 2}) == {
        "a": 1,
        "b": 2,
    }


def test_online_write_batch_raises_on_per_record_error():
    """A partial batch failure (one record's result != 0) must raise rather
    than silently succeed — otherwise downstream sees "model saw stale
    features" weeks after the fact."""
    config = _aerospike_repo_config()
    fv = SimpleNamespace(name="fv")
    store = AerospikeOnlineStore()
    fake_client = MagicMock()

    def fake_batch_write(batch):
        # Simulate a one-partition timeout on the single record.
        batch.batch_records[0].result = 9  # TIMEOUT

    fake_client.batch_write.side_effect = fake_batch_write
    store._client = fake_client

    row = (
        _entity_key("id", 1),
        {"x": ValueProto(int64_val=1)},
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        None,
    )
    with pytest.raises(RuntimeError, match="non-OK"):
        store.online_write_batch(config, fv, [row], progress=None)


# ---------------------------------------------------------------------------
# Admin paths: update / teardown
# ---------------------------------------------------------------------------


def test_update_no_op_when_nothing_to_delete():
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    store._client = fake_client
    store.update(_aerospike_repo_config(), [], [], [], [], partial=False)
    assert not fake_client.scan.called


def test_update_single_fv_issues_single_background_scan():
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    fake_scan = MagicMock()
    fake_client.scan.return_value = fake_scan
    store._client = fake_client

    store.update(
        _aerospike_repo_config(),
        [SimpleNamespace(name="old_fv")],
        [],
        [],
        [],
        partial=False,
    )

    assert fake_client.scan.call_args[0] == ("feast", "demo_latest")
    ops = fake_scan.add_ops.call_args[0][0]
    assert len(ops) == 2
    assert {op["bin"] for op in ops} == {"features", "event_ts"}
    for op in ops:
        assert op["key"] == "old_fv"
        assert op["return_type"] == aerospike.MAP_RETURN_NONE
    fake_scan.execute_background.assert_called_once()


def test_update_multi_fv_coalesces_into_one_scan():
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    fake_scan = MagicMock()
    fake_client.scan.return_value = fake_scan
    store._client = fake_client

    tables = [SimpleNamespace(name=n) for n in ("a", "b", "c")]
    store.update(_aerospike_repo_config(), tables, [], [], [], partial=False)

    fake_client.scan.assert_called_once()
    ops = fake_scan.add_ops.call_args[0][0]
    assert len(ops) == 6
    assert {op["bin"] for op in ops} == {"features", "event_ts"}
    assert {op["key"] for op in ops} == {"a", "b", "c"}
    fake_scan.execute_background.assert_called_once()


def test_update_rejects_non_aerospike_config():
    wrong_config = RepoConfig(
        project="demo",
        provider="local",
        registry="/tmp/reg.db",
        online_store={"type": "sqlite", "path": "/tmp/online.db"},
        entity_key_serialization_version=3,
    )
    store = AerospikeOnlineStore()
    with pytest.raises(RuntimeError):
        store.update(
            wrong_config,
            [SimpleNamespace(name="x")],
            [],
            [],
            [],
            partial=False,
        )


def test_teardown_truncates_and_closes_client():
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    store._client = fake_client
    store.teardown(_aerospike_repo_config(), [], [])
    assert fake_client.truncate.call_args[0] == ("feast", "demo_latest", 0)
    fake_client.close.assert_called_once()
    assert store._client is None


def test_teardown_rejects_non_aerospike_config():
    wrong_config = RepoConfig(
        project="demo",
        provider="local",
        registry="/tmp/reg.db",
        online_store={"type": "sqlite", "path": "/tmp/online.db"},
        entity_key_serialization_version=3,
    )
    store = AerospikeOnlineStore()
    with pytest.raises(RuntimeError):
        store.teardown(wrong_config, [], [])


# ---------------------------------------------------------------------------
# Per-feature-view namespace + set overrides
# ---------------------------------------------------------------------------
# These let a deployment pin individual feature views to RAM-only / SSD-backed
# namespaces or isolate one view in its own set without splitting projects.
# Coverage:
#   * the ``_namespace_for_fv`` / ``_set_name`` resolvers
#   * read + write paths actually use the resolved ns/set on the wire
#   * ``update()`` issues one scan per (ns, set) group
#   * ``teardown()`` truncates every unique (ns, set) pair


def test_set_name_default_when_no_overrides():
    config = _aerospike_repo_config()
    store = AerospikeOnlineStore()
    assert store._set_name(config) == "demo_latest"
    assert store._set_name(config, "any_fv") == "demo_latest"


def test_set_name_resolves_per_fv_override():
    config = _aerospike_repo_config(
        set_overrides={"hot_fv": "demo_hot", "cold_fv": "demo_cold"}
    )
    store = AerospikeOnlineStore()
    assert store._set_name(config, "hot_fv") == "demo_hot"
    assert store._set_name(config, "cold_fv") == "demo_cold"
    # Unmapped FV still falls back to the project-level default.
    assert store._set_name(config, "other_fv") == "demo_latest"
    # Calling without an FV name explicitly asks for the project default.
    assert store._set_name(config) == "demo_latest"


def test_namespace_for_fv_default_when_no_overrides():
    config = _aerospike_repo_config()
    store = AerospikeOnlineStore()
    assert store._namespace_for_fv(config) == "feast"
    assert store._namespace_for_fv(config, "any_fv") == "feast"


def test_namespace_for_fv_resolves_per_fv_override():
    config = _aerospike_repo_config(
        namespace_overrides={"hot_fv": "feast_ram", "cold_fv": "feast_ssd"}
    )
    store = AerospikeOnlineStore()
    assert store._namespace_for_fv(config, "hot_fv") == "feast_ram"
    assert store._namespace_for_fv(config, "cold_fv") == "feast_ssd"
    assert store._namespace_for_fv(config, "other_fv") == "feast"


def test_aerospike_key_honours_overrides_when_fv_passed():
    """``_aerospike_key`` is the choke point for everything that builds an
    Aerospike (ns, set, user_key) tuple — it must respect the overrides
    when an FV name is provided, and stay backwards-compatible without."""
    config = _aerospike_repo_config(
        namespace_overrides={"hot_fv": "feast_ram"},
        set_overrides={"hot_fv": "demo_hot"},
    )
    store = AerospikeOnlineStore()
    ek = _entity_key("driver_id", 1)

    default_ns, default_set, _ = store._aerospike_key(config, ek)
    assert (default_ns, default_set) == ("feast", "demo_latest")

    hot_ns, hot_set, _ = store._aerospike_key(config, ek, fv_name="hot_fv")
    assert (hot_ns, hot_set) == ("feast_ram", "demo_hot")


def test_online_write_batch_writes_to_per_fv_ns_and_set():
    """The wire keys produced by online_write_batch must use the FV's
    resolved ns + set, not the store-level defaults."""
    config = _aerospike_repo_config(
        namespace_overrides={"driver_stats": "feast_ram"},
        set_overrides={"driver_stats": "demo_hot"},
    )
    fv = SimpleNamespace(name="driver_stats")
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    captured: dict = {}

    def fake_batch_write(batch):
        captured["batch"] = batch

    fake_client.batch_write.side_effect = fake_batch_write
    store._client = fake_client

    row = (
        _entity_key("id", 1),
        {"x": ValueProto(int64_val=1)},
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        None,
    )
    store.online_write_batch(config, fv, [row], progress=None)

    bw = captured["batch"].batch_records[0]
    assert bw.key[:2] == ("feast_ram", "demo_hot")


def test_online_read_uses_per_fv_ns_and_set():
    """The ``batch_operate`` keys must use the FV's resolved ns + set."""
    config = _aerospike_repo_config(
        namespace_overrides={"driver_stats": "feast_ram"},
        set_overrides={"driver_stats": "demo_hot"},
    )
    fv = _read_feature_view()  # name="driver_stats"
    store = AerospikeOnlineStore()
    captured: dict = {}

    def fake_batch_operate(keys, ops):
        captured["keys"] = keys
        return SimpleNamespace(batch_records=[_fake_batch_record(keys[0], None)])

    fake_client = MagicMock()
    fake_client.batch_operate.side_effect = fake_batch_operate
    store._client = fake_client

    store.online_read(config, fv, [_entity_key("driver_id", 1)])
    assert len(captured["keys"]) == 1
    assert captured["keys"][0][:2] == ("feast_ram", "demo_hot")


def test_update_groups_dropped_fvs_by_resolved_ns_and_set():
    """Dropped feature views in different (ns, set) buckets must produce
    one background scan per bucket — a single combined scan would either
    miss records or pointlessly scan unrelated namespaces."""
    config = _aerospike_repo_config(
        namespace_overrides={"a": "ns_x"},  # b, c land on default ns
        set_overrides={"b": "set_y"},  # a, c land on default set
    )
    store = AerospikeOnlineStore()
    fake_client = MagicMock()

    fake_scans: list = []

    def make_scan(ns, set_name):
        s = MagicMock(name=f"scan_{ns}_{set_name}")
        s._ns = ns
        s._set = set_name
        fake_scans.append(s)
        return s

    fake_client.scan.side_effect = make_scan
    store._client = fake_client

    tables = [
        SimpleNamespace(name="a"),  # (ns_x, demo_latest)
        SimpleNamespace(name="b"),  # (feast,  set_y)
        SimpleNamespace(name="c"),  # (feast,  demo_latest)
    ]
    store.update(config, tables, [], [], [], partial=False)

    targets = {(s._ns, s._set) for s in fake_scans}
    assert targets == {
        ("ns_x", "demo_latest"),
        ("feast", "set_y"),
        ("feast", "demo_latest"),
    }
    # Every group's scan must execute and ship the matching FVs' remove ops.
    by_target = {(s._ns, s._set): s for s in fake_scans}
    for tgt, fv_name in [
        (("ns_x", "demo_latest"), "a"),
        (("feast", "set_y"), "b"),
        (("feast", "demo_latest"), "c"),
    ]:
        scan = by_target[tgt]
        ops = scan.add_ops.call_args[0][0]
        assert {op["bin"] for op in ops} == {"features", "event_ts"}
        assert all(op["key"] == fv_name for op in ops)
        scan.execute_background.assert_called_once()


def test_update_coalesces_fvs_sharing_a_resolved_target():
    """Two dropped FVs that resolve to the same (ns, set) must share one
    scan — the per-FV grouping is "by target", not "by FV name"."""
    config = _aerospike_repo_config(
        namespace_overrides={"a": "ns_x", "b": "ns_x"},
        set_overrides={"a": "set_y", "b": "set_y"},
    )
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    fake_scan = MagicMock()
    fake_client.scan.return_value = fake_scan
    store._client = fake_client

    store.update(
        config,
        [SimpleNamespace(name="a"), SimpleNamespace(name="b")],
        [],
        [],
        [],
        partial=False,
    )

    fake_client.scan.assert_called_once_with("ns_x", "set_y")
    ops = fake_scan.add_ops.call_args[0][0]
    # 4 ops total: features+event_ts for each of the 2 FVs.
    assert len(ops) == 4
    assert {op["key"] for op in ops} == {"a", "b"}


def test_teardown_truncates_default_plus_every_overridden_pair():
    config = _aerospike_repo_config(
        namespace_overrides={"hot": "feast_ram"},
        set_overrides={"isolated": "demo_iso"},
    )
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    store._client = fake_client

    tables = [
        SimpleNamespace(name="hot"),  # (feast_ram, demo_latest)
        SimpleNamespace(name="isolated"),  # (feast,    demo_iso)
        SimpleNamespace(name="default"),  # (feast,    demo_latest)
    ]
    store.teardown(config, tables, [])

    truncated = {tuple(call.args[:2]) for call in fake_client.truncate.call_args_list}
    assert truncated == {
        ("feast", "demo_latest"),
        ("feast_ram", "demo_latest"),
        ("feast", "demo_iso"),
    }
    # cutoff is always 0 ("drop everything regardless of last-update time")
    for call in fake_client.truncate.call_args_list:
        assert call.args[2] == 0
    fake_client.close.assert_called_once()


def test_teardown_with_empty_tables_still_clears_default_pair():
    """A teardown call with an empty tables list (e.g. a bare ``feast
    teardown`` on a repo whose registry is empty) must still clear the
    store-default location, otherwise stale data lingers."""
    config = _aerospike_repo_config(
        namespace_overrides={"unused": "feast_ram"},
    )
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    store._client = fake_client

    store.teardown(config, [], [])

    truncated = {tuple(call.args[:2]) for call in fake_client.truncate.call_args_list}
    assert truncated == {("feast", "demo_latest")}


# ---------------------------------------------------------------------------
# Prewriting hook
# ---------------------------------------------------------------------------
# Hook-target functions must be module-level so the import-string resolver
# can find them via importlib + getattr. They're referenced by f"{__name__}.X".


def _hook_uppercase_string_features(config, table, data):  # noqa: ARG001
    """Sample hook: uppercase every string ValueProto.string_val on every row."""
    new_data = []
    for entity_key, values, ts, created in data:
        new_values = {}
        for k, v in values.items():
            if v.HasField("string_val"):
                new_values[k] = ValueProto(string_val=v.string_val.upper())
            else:
                new_values[k] = v
        new_data.append((entity_key, new_values, ts, created))
    return new_data


def _hook_drop_all_rows(config, table, data):  # noqa: ARG001
    """Sample hook that filters every row — exercises the post-hook empty-batch path."""
    return []


def _hook_raises(config, table, data):  # noqa: ARG001
    raise ValueError("hook intentionally failed")


_NOT_A_FUNCTION = 42


def test_prewriting_hook_unset_returns_none():
    config = _aerospike_repo_config()  # no prewriting_hook
    store = AerospikeOnlineStore()
    assert store._resolve_prewriting_hook(config) is None
    # Must clear any previously-cached value so a config swap takes effect.
    store._prewriting_hook = lambda *a, **kw: None  # type: ignore[assignment]
    store._prewriting_hook_spec = "stale.spec"
    store._resolve_prewriting_hook(config)
    assert store._prewriting_hook is None
    assert store._prewriting_hook_spec is None


def test_prewriting_hook_resolves_and_caches():
    spec = f"{__name__}._hook_uppercase_string_features"
    config = _aerospike_repo_config(prewriting_hook=spec)
    store = AerospikeOnlineStore()

    hook = store._resolve_prewriting_hook(config)
    assert hook is _hook_uppercase_string_features

    # Second call must hit the cache (no re-import).
    with patch(
        "feast.infra.online_stores.aerospike_online_store.aerospike.importlib"
    ) as m:
        again = store._resolve_prewriting_hook(config)
        assert again is hook
        assert not m.import_module.called


def test_prewriting_hook_recompiles_after_spec_change():
    """If the config is rebound to a new hook string, the resolver must
    re-resolve instead of returning the cached old callable."""
    store = AerospikeOnlineStore()

    cfg1 = _aerospike_repo_config(
        prewriting_hook=f"{__name__}._hook_uppercase_string_features"
    )
    assert store._resolve_prewriting_hook(cfg1) is _hook_uppercase_string_features

    cfg2 = _aerospike_repo_config(prewriting_hook=f"{__name__}._hook_drop_all_rows")
    assert store._resolve_prewriting_hook(cfg2) is _hook_drop_all_rows


def test_prewriting_hook_bad_format_raises():
    config = _aerospike_repo_config(prewriting_hook="no_dot_here")
    store = AerospikeOnlineStore()
    with pytest.raises(ValueError, match="fully qualified import path"):
        store._resolve_prewriting_hook(config)


def test_prewriting_hook_missing_module_raises():
    config = _aerospike_repo_config(prewriting_hook="definitely_not_a_real.module.fn")
    store = AerospikeOnlineStore()
    with pytest.raises(ValueError, match="could not import module"):
        store._resolve_prewriting_hook(config)


def test_prewriting_hook_missing_attribute_raises():
    config = _aerospike_repo_config(prewriting_hook=f"{__name__}._does_not_exist")
    store = AerospikeOnlineStore()
    with pytest.raises(ValueError, match="has no attribute"):
        store._resolve_prewriting_hook(config)


def test_prewriting_hook_non_callable_raises():
    config = _aerospike_repo_config(prewriting_hook=f"{__name__}._NOT_A_FUNCTION")
    store = AerospikeOnlineStore()
    with pytest.raises(TypeError, match="non-callable"):
        store._resolve_prewriting_hook(config)


def test_prewriting_hook_transforms_data_before_write():
    """End-to-end: the hook's output is what lands on the wire, not the
    caller-supplied data."""
    config = _aerospike_repo_config(
        prewriting_hook=f"{__name__}._hook_uppercase_string_features"
    )
    fv = SimpleNamespace(name="fv")
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    captured: dict = {}

    def fake_batch_write(batch):
        captured["batch"] = batch

    fake_client.batch_write.side_effect = fake_batch_write
    store._client = fake_client

    row = (
        _entity_key("id", 1),
        {"name": ValueProto(string_val="alice")},
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        None,
    )
    store.online_write_batch(config, fv, [row], progress=None)

    bw = captured["batch"].batch_records[0]
    map_put_op = next(op for op in bw.ops if op["bin"] == "features")
    # The hook upper-cases string features. ``map_put_items`` stores its
    # payload under the ``val`` key (mapping FV name -> feature submap).
    fv_map = map_put_op["val"]["fv"]
    assert fv_map == {"name": "ALICE"}


def test_prewriting_hook_returning_empty_short_circuits():
    """A hook that filters every row must skip the wire call and report
    progress=0 — same shape as the empty-input fast path."""
    config = _aerospike_repo_config(prewriting_hook=f"{__name__}._hook_drop_all_rows")
    fv = SimpleNamespace(name="fv")
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    store._client = fake_client

    progress_calls: list[int] = []
    row = (
        _entity_key("id", 1),
        {"x": ValueProto(int64_val=1)},
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        None,
    )
    store.online_write_batch(config, fv, [row], progress=progress_calls.append)

    assert not fake_client.batch_write.called
    assert progress_calls == [0]


def test_prewriting_hook_propagates_exceptions():
    """A raising hook must fail the whole batch — there's no per-row
    fallback. ``batch_write`` must never be called."""
    config = _aerospike_repo_config(prewriting_hook=f"{__name__}._hook_raises")
    fv = SimpleNamespace(name="fv")
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    store._client = fake_client

    row = (
        _entity_key("id", 1),
        {"x": ValueProto(int64_val=1)},
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        None,
    )
    with pytest.raises(ValueError, match="hook intentionally failed"):
        store.online_write_batch(config, fv, [row], progress=None)
    assert not fake_client.batch_write.called


def test_prewriting_hook_skipped_for_empty_input():
    """The hook resolver must not even be invoked when ``data`` is empty —
    avoids paying the import cost for no-op writes."""
    config = _aerospike_repo_config(
        prewriting_hook="should.never.resolve"  # would raise if resolved
    )
    fv = SimpleNamespace(name="fv")
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    store._client = fake_client

    progress_calls: list[int] = []
    store.online_write_batch(config, fv, [], progress=progress_calls.append)
    assert not fake_client.batch_write.called
    assert progress_calls == [0]


# ---------------------------------------------------------------------------
# Async wrappers (run_in_executor)
# ---------------------------------------------------------------------------


async def test_online_write_batch_async_delegates_to_sync():
    """The async write path must produce identical side-effects to the sync one."""
    config = _aerospike_repo_config()
    fv = SimpleNamespace(name="fv")
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    store._client = fake_client

    row = (
        _entity_key("id", 1),
        {"x": ValueProto(int64_val=1)},
        datetime(2026, 1, 1, tzinfo=timezone.utc),
        None,
    )
    progress_calls: list[int] = []
    await store.online_write_batch_async(
        config, fv, [row], progress=progress_calls.append
    )

    assert fake_client.batch_write.called
    assert progress_calls == [1]


async def test_online_read_async_returns_same_shape_as_sync():
    config = _aerospike_repo_config()
    fv = _read_feature_view()
    store = AerospikeOnlineStore()
    ts = datetime(2026, 4, 20, tzinfo=timezone.utc)
    ek = _entity_key("driver_id", 1)
    key = store._aerospike_key(config, ek)

    def fake_batch_operate(keys, ops):
        return SimpleNamespace(
            batch_records=[
                _fake_batch_record(
                    key,
                    {
                        "features": {"rating": 4.91, "trips_last_7d": 132},
                        "event_ts": _datetime_to_epoch_ms(ts),
                    },
                )
            ]
        )

    fake_client = MagicMock()
    fake_client.batch_operate.side_effect = fake_batch_operate
    store._client = fake_client

    results = await store.online_read_async(config, fv, [ek])
    assert len(results) == 1
    ts_out, feats = results[0]
    assert ts_out == ts
    assert feats["trips_last_7d"].int64_val == 132


async def test_initialize_pre_warms_client():
    """initialize() must cause the client to connect without needing a read/write."""
    config = _aerospike_repo_config()
    store = AerospikeOnlineStore()
    assert store._client is None

    sentinel = MagicMock(name="warm_client")
    store._get_client = MagicMock(return_value=sentinel)  # type: ignore[assignment]

    await store.initialize(config)
    store._get_client.assert_called_once_with(config)


async def test_close_is_noop_without_client():
    store = AerospikeOnlineStore()
    assert store._client is None
    await store.close()  # must not raise


async def test_close_releases_client():
    store = AerospikeOnlineStore()
    fake_client = MagicMock()
    store._client = fake_client

    await store.close()

    fake_client.close.assert_called_once()
    assert store._client is None


# ---------------------------------------------------------------------------
# End-to-end integration test — requires Docker
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def aerospike_container():
    """Start a real Aerospike CE container for end-to-end testing."""
    container = DockerContainer(AEROSPIKE_CE_IMAGE).with_exposed_ports("3000")
    container.start()
    wait_for_logs(container=container, predicate="migrations: complete", timeout=60)
    yield container
    container.stop()


@pytest.fixture
def aerospike_online_store_config(aerospike_container):
    port = int(aerospike_container.get_exposed_port("3000"))
    return {
        "type": "aerospike",
        "hosts": [("127.0.0.1", port)],
        "namespace": "test",  # default namespace shipped in the CE image
    }


@_requires_docker
def test_aerospike_online_features(aerospike_online_store_config):
    """Full round-trip: write via the provider, read via the feature store API."""
    runner = CliRunner()
    with runner.local_repo(
        get_example_repo("example_feature_repo_1.py"),
        offline_store="file",
        online_store="aerospike",
        teardown=False,  # container torn down by fixture
    ) as store:
        # Patch in the live container's port.
        store.config.online_store.hosts = aerospike_online_store_config["hosts"]
        store.config.online_store.namespace = aerospike_online_store_config["namespace"]

        driver_locations_fv = store.get_feature_view(name="driver_locations")
        customer_profile_fv = store.get_feature_view(name="customer_profile")
        customer_driver_combined_fv = store.get_feature_view(
            name="customer_driver_combined"
        )

        provider = store._get_provider()

        driver_key = EntityKeyProto(
            join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1)]
        )
        provider.online_write_batch(
            config=store.config,
            table=driver_locations_fv,
            data=[
                (
                    driver_key,
                    {
                        "lat": ValueProto(double_val=0.1),
                        "lon": ValueProto(string_val="1.0"),
                    },
                    _utc_now(),
                    _utc_now(),
                )
            ],
            progress=None,
        )

        customer_key = EntityKeyProto(
            join_keys=["customer_id"], entity_values=[ValueProto(string_val="5")]
        )
        provider.online_write_batch(
            config=store.config,
            table=customer_profile_fv,
            data=[
                (
                    customer_key,
                    {
                        "avg_orders_day": ValueProto(float_val=1.0),
                        "name": ValueProto(string_val="John"),
                        "age": ValueProto(int64_val=3),
                    },
                    _utc_now(),
                    _utc_now(),
                )
            ],
            progress=None,
        )

        combined_key = EntityKeyProto(
            join_keys=["customer_id", "driver_id"],
            entity_values=[ValueProto(string_val="5"), ValueProto(int64_val=1)],
        )
        provider.online_write_batch(
            config=store.config,
            table=customer_driver_combined_fv,
            data=[
                (
                    combined_key,
                    {"trips": ValueProto(int64_val=7)},
                    _utc_now(),
                    _utc_now(),
                )
            ],
            progress=None,
        )

        result = store.get_online_features(
            features=[
                "driver_locations:lon",
                "customer_profile:avg_orders_day",
                "customer_profile:name",
                "customer_driver_combined:trips",
            ],
            entity_rows=[
                {"driver_id": 1, "customer_id": "5"},
                {"driver_id": 1, "customer_id": 5},
            ],
            full_feature_names=False,
        ).to_dict()

        assert result["driver_id"] == [1, 1]
        assert result["customer_id"] == ["5", "5"]
        assert result["lon"] == ["1.0", "1.0"]
        assert result["avg_orders_day"] == [1.0, 1.0]
        assert result["name"] == ["John", "John"]
        assert result["trips"] == [7, 7]

        missing = store.get_online_features(
            features=["customer_driver_combined:trips"],
            entity_rows=[{"driver_id": 0, "customer_id": 0}],
            full_feature_names=False,
        ).to_dict()
        assert missing["trips"] == [None]


# ---------------------------------------------------------------------------
# Integration tests that exercise the store directly (no CliRunner/apply).
# ---------------------------------------------------------------------------


def _integration_repo_config(
    aerospike_online_store_config: dict, collection_suffix: str
) -> RepoConfig:
    """Build a RepoConfig targeting the live container with an isolated set name.

    Each test passes a unique ``collection_suffix`` so the module-scoped
    Aerospike container can host multiple tests without cross-contamination.
    """
    return RepoConfig(
        project="itest",
        provider="local",
        registry="/tmp/reg.db",
        online_store={
            **aerospike_online_store_config,
            "collection_suffix": collection_suffix,
        },
        entity_key_serialization_version=3,
    )


def _multi_fv_feature_view(name: str) -> SimpleNamespace:
    """Minimal FV duck-typed for the store: exposes .name and .features."""
    return SimpleNamespace(
        name=name,
        features=[
            SimpleNamespace(name="value", dtype=Int64),
        ],
    )


@_requires_docker
def test_aerospike_cross_fv_map_cdt_upsert(aerospike_online_store_config):
    """Writing two feature views for the same entity must not clobber each other.

    The store uses Aerospike Map CDT ops (``map_put_items``) rather than a
    full-record ``put``, so two feature views sharing an entity key live in
    separate slots of the ``features`` / ``event_ts`` maps. This test
    exercises that guarantee end to end against a real server.
    """
    config = _integration_repo_config(aerospike_online_store_config, "cross_fv")
    store = AerospikeOnlineStore()

    fv_a = _multi_fv_feature_view("driver_stats")
    fv_b = _multi_fv_feature_view("driver_geo")
    ek = _entity_key("driver_id", 101)
    ts = _utc_now()

    store.online_write_batch(
        config,
        fv_a,
        [(ek, {"value": ValueProto(int64_val=42)}, ts, ts)],
        progress=None,
    )
    # Second write targets the SAME entity key under a different feature
    # view — it must add a new map entry rather than overwrite fv_a.
    store.online_write_batch(
        config,
        fv_b,
        [(ek, {"value": ValueProto(int64_val=7)}, ts, ts)],
        progress=None,
    )

    results_a = store.online_read(config, fv_a, [ek])
    results_b = store.online_read(config, fv_b, [ek])

    assert len(results_a) == 1 and len(results_b) == 1
    _, feats_a = results_a[0]
    _, feats_b = results_b[0]
    assert feats_a is not None, "driver_stats was clobbered by the driver_geo write"
    assert feats_b is not None, "driver_geo write did not produce a readable slot"
    assert feats_a["value"].int64_val == 42
    assert feats_b["value"].int64_val == 7

    # Sanity-check the raw record too: both feature views should coexist in
    # the ``features`` Map CDT under their own names.
    client = store._get_client(config)
    _, _, bins = client.get(store._aerospike_key(config, ek))
    assert set(bins["features"].keys()) == {"driver_stats", "driver_geo"}
    assert set(bins["event_ts"].keys()) == {"driver_stats", "driver_geo"}

    store.teardown(config, [], [])


@_requires_docker
def test_aerospike_update_strips_dropped_feature_view(aerospike_online_store_config):
    """``update(tables_to_delete=[...])`` removes a FV's slot from every record.

    The store issues a single background scan that applies
    ``map_remove_by_key`` to the ``features`` and ``event_ts`` bins for each
    dropped feature view. We verify:

    * The dropped FV's slot disappears from the record (read returns
      ``(None, None)``).
    * The kept FV is untouched (still readable with its original values).
    * The underlying record itself still exists (background scan strips map
      entries, not whole records).
    """
    config = _integration_repo_config(aerospike_online_store_config, "update")
    store = AerospikeOnlineStore()

    fv_keep = _multi_fv_feature_view("keep_fv")
    fv_drop = _multi_fv_feature_view("drop_fv")
    ek = _entity_key("driver_id", 202)
    ts = _utc_now()

    store.online_write_batch(
        config,
        fv_keep,
        [(ek, {"value": ValueProto(int64_val=1)}, ts, ts)],
        progress=None,
    )
    store.online_write_batch(
        config,
        fv_drop,
        [(ek, {"value": ValueProto(int64_val=999)}, ts, ts)],
        progress=None,
    )

    # Pre-condition: both feature views readable.
    assert store.online_read(config, fv_keep, [ek])[0][1] is not None
    assert store.online_read(config, fv_drop, [ek])[0][1] is not None

    store.update(
        config=config,
        tables_to_delete=[fv_drop],
        tables_to_keep=[fv_keep],
        entities_to_delete=[],
        entities_to_keep=[],
        partial=False,
    )

    # The background scan is asynchronous server-side. Poll up to ~10s for
    # the drop_fv slot to disappear before failing — this matches how a real
    # Feast caller would experience the post-apply propagation delay.
    deadline = time.monotonic() + 10.0
    drop_result = store.online_read(config, fv_drop, [ek])[0]
    while drop_result != (None, None) and time.monotonic() < deadline:
        time.sleep(0.1)
        drop_result = store.online_read(config, fv_drop, [ek])[0]
    assert drop_result == (None, None), (
        f"drop_fv slot was not cleared by background scan within 10s; "
        f"last result: {drop_result!r}"
    )

    # keep_fv must still be present with its original value.
    keep_ts, keep_feats = store.online_read(config, fv_keep, [ek])[0]
    assert keep_feats is not None, "keep_fv was removed by the scan"
    assert keep_feats["value"].int64_val == 1
    assert keep_ts is not None

    # The record itself still exists — only the dropped FV's map entries
    # were removed. Verify directly via a raw get so we notice if a future
    # change accidentally escalates to a full-record delete.
    client = store._get_client(config)
    _, _, bins = client.get(store._aerospike_key(config, ek))
    assert "drop_fv" not in bins["features"]
    assert "drop_fv" not in bins["event_ts"]
    assert "keep_fv" in bins["features"]

    store.teardown(config, [], [])
