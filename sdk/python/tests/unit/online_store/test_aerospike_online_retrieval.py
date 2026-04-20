"""
Unit tests for the Aerospike online store.

Most of the tests here are pure Python and run in any environment (they cover
the timestamp/TTL helpers, the column-oriented proto reshape, and the
write/read/admin dispatch with a mocked Aerospike client). One end-to-end test
is marked with ``@_requires_docker`` and is skipped when Docker is unavailable.
"""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

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
        config, fv, [row], set_name="demo_latest"
    )

    assert len(batch.batch_records) == 1
    bw = batch.batch_records[0]
    assert bw.key[:2] == ("feast", "demo_latest")
    assert isinstance(bw.key[2], bytes)
    assert bw.meta == {"ttl": 3600}
    assert bw.policy == {"key": aerospike.POLICY_KEY_SEND}
    assert len(bw.ops) == 3
    bin_names = [op["bin"] for op in bw.ops]
    assert bin_names == ["features", "event_ts", "created_ts"]


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
        config, fv, [row], set_name="demo_latest"
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

    batch = AerospikeOnlineStore._build_batch_writes(config, fv, [row], "set")
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
        # Return records in a *shuffled* order to prove the output still respects input order.
        br1 = _fake_batch_record(
            key1,
            {
                "features": {"rating": 4.91, "trips_last_7d": 132},
                "event_ts": _datetime_to_epoch_ms(ts),
            },
        )
        br3 = _fake_batch_record(
            key3,
            {
                "features": {"rating": 3.75, "trips_last_7d": 42},
                "event_ts": _datetime_to_epoch_ms(ts),
            },
        )
        br2 = _fake_batch_record(key2, None)  # missing record
        return SimpleNamespace(batch_records=[br3, br1, br2])

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
# End-to-end integration test — requires Docker
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def aerospike_container():
    """Start a real Aerospike CE container for end-to-end testing."""
    container = DockerContainer("aerospike:ce-8.0.0.9_1").with_exposed_ports("3000")
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
