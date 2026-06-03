"""Tests for pre-computed feature vectors (issue #6185)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.protos.feast.core.PrecomputedFeatureVector_pb2 import (
    FeatureViewTimestamp,
    PrecomputedFeatureVector,
)
from feast.protos.feast.serving.ServingService_pb2 import (
    FieldStatus,
    GetOnlineFeaturesResponse,
)
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Float32, Int64


def _make_file_source(name="src"):
    return FileSource(name=name, path="test.parquet")


def _make_fv(name, features, source=None, ttl=None):
    return FeatureView(
        name=name,
        entities=[],
        schema=features,
        source=source or _make_file_source(f"{name}_src"),
        ttl=ttl or timedelta(0),
    )


def _make_timestamp(dt=None):
    ts = Timestamp()
    ts.FromDatetime(dt or datetime.now(tz=timezone.utc))
    return ts


def _make_vector(feature_names, values, fv_timestamps=None, precomputed_at=None):
    return PrecomputedFeatureVector(
        feature_names=feature_names,
        values=values,
        fv_timestamps=fv_timestamps or [],
        precomputed_at=precomputed_at or _make_timestamp(),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Proto round-trip
# ═══════════════════════════════════════════════════════════════════════════════


class TestPrecomputedFeatureVectorProto:
    def test_serialize_deserialize(self):
        now = _make_timestamp()
        fv_ts = FeatureViewTimestamp(feature_view_name="driver_fv", event_timestamp=now)
        vector = _make_vector(
            feature_names=["driver_fv__trips", "driver_fv__rating"],
            values=[ValueProto(int64_val=100), ValueProto(float_val=4.5)],
            fv_timestamps=[fv_ts],
            precomputed_at=now,
        )

        blob = vector.SerializeToString()
        parsed = PrecomputedFeatureVector()
        parsed.ParseFromString(blob)

        assert list(parsed.feature_names) == ["driver_fv__trips", "driver_fv__rating"]
        assert parsed.values[0].int64_val == 100
        assert parsed.values[1].float_val == pytest.approx(4.5)
        assert parsed.fv_timestamps[0].feature_view_name == "driver_fv"

    def test_empty_vector(self):
        vector = PrecomputedFeatureVector()
        blob = vector.SerializeToString()
        parsed = PrecomputedFeatureVector()
        parsed.ParseFromString(blob)
        assert list(parsed.feature_names) == []
        assert list(parsed.values) == []

    def test_multiple_fv_timestamps(self):
        ts1 = _make_timestamp(datetime(2025, 1, 1, tzinfo=timezone.utc))
        ts2 = _make_timestamp(datetime(2025, 6, 1, tzinfo=timezone.utc))

        vector = _make_vector(
            feature_names=["fv1__a", "fv2__b"],
            values=[ValueProto(float_val=1.0), ValueProto(float_val=2.0)],
            fv_timestamps=[
                FeatureViewTimestamp(feature_view_name="fv1", event_timestamp=ts1),
                FeatureViewTimestamp(feature_view_name="fv2", event_timestamp=ts2),
            ],
        )
        blob = vector.SerializeToString()
        parsed = PrecomputedFeatureVector()
        parsed.ParseFromString(blob)

        assert len(parsed.fv_timestamps) == 2
        assert parsed.fv_timestamps[0].feature_view_name == "fv1"
        assert parsed.fv_timestamps[1].feature_view_name == "fv2"
        assert parsed.fv_timestamps[0].event_timestamp.seconds == ts1.seconds
        assert parsed.fv_timestamps[1].event_timestamp.seconds == ts2.seconds

    def test_null_values_preserved(self):
        vector = _make_vector(
            feature_names=["fv1__a", "fv1__b"],
            values=[ValueProto(float_val=1.0), ValueProto()],
        )
        blob = vector.SerializeToString()
        parsed = PrecomputedFeatureVector()
        parsed.ParseFromString(blob)

        assert parsed.values[0].float_val == pytest.approx(1.0)
        assert not parsed.values[1].HasField("val")

    def test_large_vector(self):
        n = 100
        names = [f"fv__feat_{i}" for i in range(n)]
        vals = [ValueProto(float_val=float(i)) for i in range(n)]
        vector = _make_vector(feature_names=names, values=vals)
        blob = vector.SerializeToString()
        parsed = PrecomputedFeatureVector()
        parsed.ParseFromString(blob)

        assert len(parsed.feature_names) == n
        assert len(parsed.values) == n
        assert parsed.values[99].float_val == pytest.approx(99.0)


# ═══════════════════════════════════════════════════════════════════════════════
# FeatureService precompute_online flag
# ═══════════════════════════════════════════════════════════════════════════════


class TestFeatureServicePrecomputeOnline:
    def test_default_is_false(self):
        fs = FeatureService(name="svc", features=[])
        assert fs.precompute_online is False

    def test_set_true(self):
        fs = FeatureService(name="svc", features=[], precompute_online=True)
        assert fs.precompute_online is True

    def test_to_proto_roundtrip_true(self):
        fv = _make_fv("fv1", [Field(name="f1", dtype=Float32)])
        fs = FeatureService(name="svc", features=[fv], precompute_online=True)
        proto = fs.to_proto()
        assert proto.spec.precompute_online is True
        restored = FeatureService.from_proto(proto)
        assert restored.precompute_online is True

    def test_to_proto_roundtrip_false(self):
        fs = FeatureService(name="svc", features=[], precompute_online=False)
        proto = fs.to_proto()
        assert proto.spec.precompute_online is False
        restored = FeatureService.from_proto(proto)
        assert restored.precompute_online is False

    def test_equality_includes_precompute(self):
        fv = _make_fv("fv1", [Field(name="f1", dtype=Float32)])
        fs1 = FeatureService(name="svc", features=[fv], precompute_online=True)
        fs2 = FeatureService(name="svc", features=[fv], precompute_online=False)
        assert fs1 != fs2

    def test_equality_same_precompute(self):
        fv = _make_fv("fv1", [Field(name="f1", dtype=Float32)])
        fs1 = FeatureService(name="svc", features=[fv], precompute_online=True)
        fs2 = FeatureService(name="svc", features=[fv], precompute_online=True)
        assert fs1 == fs2

    def test_proto_preserves_other_fields(self):
        fv = _make_fv("fv1", [Field(name="f1", dtype=Float32)])
        fs = FeatureService(
            name="svc",
            features=[fv],
            precompute_online=True,
            description="test desc",
            owner="owner@test.com",
            tags={"env": "prod"},
        )
        proto = fs.to_proto()
        restored = FeatureService.from_proto(proto)

        assert restored.precompute_online is True
        assert restored.description == "test desc"
        assert restored.owner == "owner@test.com"
        assert restored.tags == {"env": "prod"}


# ═══════════════════════════════════════════════════════════════════════════════
# Validation
# ═══════════════════════════════════════════════════════════════════════════════


class TestFeatureServiceValidation:
    def test_validate_passes_without_precompute(self):
        fs = FeatureService(name="svc", features=[], precompute_online=False)
        fs.validate()

    def test_validate_passes_precompute_regular_fv(self):
        fv = _make_fv("fv1", [Field(name="f1", dtype=Float32)])
        fs = FeatureService(name="svc", features=[fv], precompute_online=True)
        fs.validate()

    def test_validate_rejects_odfv_without_write_to_online(self):
        fv = _make_fv("fv1", [Field(name="f1", dtype=Float32)])

        odfv = MagicMock(spec=OnDemandFeatureView)
        odfv.name = "odfv1"
        odfv.write_to_online_store = False

        fs = FeatureService(name="svc", features=[fv], precompute_online=True)
        fs._features = [fv, odfv]

        with pytest.raises(ValueError, match="write_to_online_store=False"):
            fs.validate()

    def test_validate_passes_odfv_with_write_to_online(self):
        fv = _make_fv("fv1", [Field(name="f1", dtype=Float32)])

        odfv = MagicMock(spec=OnDemandFeatureView)
        odfv.name = "odfv1"
        odfv.write_to_online_store = True

        fs = FeatureService(name="svc", features=[fv], precompute_online=True)
        fs._features = [fv, odfv]

        fs.validate()

    def test_validate_rejects_multiple_odfvs_without_write(self):
        odfv1 = MagicMock(spec=OnDemandFeatureView)
        odfv1.name = "odfv1"
        odfv1.write_to_online_store = False

        odfv2 = MagicMock(spec=OnDemandFeatureView)
        odfv2.name = "odfv2"
        odfv2.write_to_online_store = False

        fs = FeatureService(name="svc", features=[], precompute_online=True)
        fs._features = [odfv1, odfv2]

        with pytest.raises(ValueError, match="odfv1"):
            fs.validate()

    def test_validate_error_message_includes_service_and_odfv_name(self):
        odfv = MagicMock(spec=OnDemandFeatureView)
        odfv.name = "my_transform"
        odfv.write_to_online_store = False

        fs = FeatureService(name="my_service", features=[], precompute_online=True)
        fs._features = [odfv]

        with pytest.raises(ValueError) as exc_info:
            fs.validate()

        assert "my_service" in str(exc_info.value)
        assert "my_transform" in str(exc_info.value)


# ═══════════════════════════════════════════════════════════════════════════════
# Schema mismatch detection (fast path fallback)
# ═══════════════════════════════════════════════════════════════════════════════


class TestSchemaMismatchFallback:
    def _fast_path(self, blobs, expected_names, grouped_refs=None, num_rows=None):
        from feast.infra.online_stores.online_store import OnlineStore

        response = GetOnlineFeaturesResponse(results=[])
        result = OnlineStore._try_precomputed_fast_path(
            blobs=blobs,
            expected_feature_names=expected_names,
            online_features_response=response,
            full_feature_names=True,
            num_rows=num_rows or len(blobs),
            grouped_refs=grouped_refs or [],
            registry=MagicMock(),
            project="test",
        )
        return result, response

    def test_mismatch_returns_false(self):
        vector = _make_vector(
            feature_names=["fv1__f1", "fv1__f2"],
            values=[ValueProto(float_val=1.0), ValueProto(float_val=2.0)],
        )
        ok, _ = self._fast_path([vector.SerializeToString()], ["fv1__f1", "fv1__f3"])
        assert ok is False

    def test_none_blob_returns_false(self):
        ok, _ = self._fast_path([None], ["fv1__f1"])
        assert ok is False

    def test_matching_schema_returns_true(self):
        vector = _make_vector(
            feature_names=["fv1__f1"],
            values=[ValueProto(float_val=42.0)],
        )
        ok, response = self._fast_path([vector.SerializeToString()], ["fv1__f1"])
        assert ok is True
        assert len(response.results) == 1
        assert response.results[0].values[0].float_val == pytest.approx(42.0)

    def test_extra_feature_in_expected_returns_false(self):
        vector = _make_vector(
            feature_names=["fv1__f1"],
            values=[ValueProto(float_val=1.0)],
        )
        ok, _ = self._fast_path([vector.SerializeToString()], ["fv1__f1", "fv1__f2"])
        assert ok is False

    def test_fewer_features_in_expected_returns_false(self):
        vector = _make_vector(
            feature_names=["fv1__f1", "fv1__f2"],
            values=[ValueProto(float_val=1.0), ValueProto(float_val=2.0)],
        )
        ok, _ = self._fast_path([vector.SerializeToString()], ["fv1__f1"])
        assert ok is False

    def test_reordered_features_succeeds_with_correct_values(self):
        vector = _make_vector(
            feature_names=["fv1__f1", "fv1__f2"],
            values=[ValueProto(float_val=1.0), ValueProto(float_val=2.0)],
        )
        ok, response = self._fast_path(
            [vector.SerializeToString()], ["fv1__f2", "fv1__f1"]
        )
        assert ok is True
        assert response.results[0].values[0].float_val == pytest.approx(2.0)
        assert response.results[1].values[0].float_val == pytest.approx(1.0)

    def test_all_none_blobs_returns_false(self):
        ok, _ = self._fast_path([None, None, None], ["fv1__f1"], num_rows=3)
        assert ok is False

    def test_mixed_none_and_valid_returns_false(self):
        vector = _make_vector(
            feature_names=["fv1__f1"],
            values=[ValueProto(float_val=1.0)],
        )
        ok, _ = self._fast_path(
            [vector.SerializeToString(), None], ["fv1__f1"], num_rows=2
        )
        assert ok is False

    def test_schema_mismatch_in_second_entity_returns_false(self):
        vec1 = _make_vector(
            feature_names=["fv1__f1"], values=[ValueProto(float_val=1.0)]
        )
        vec2 = _make_vector(
            feature_names=["fv1__f_WRONG"], values=[ValueProto(float_val=2.0)]
        )
        ok, _ = self._fast_path(
            [vec1.SerializeToString(), vec2.SerializeToString()],
            ["fv1__f1"],
            num_rows=2,
        )
        assert ok is False


# ═══════════════════════════════════════════════════════════════════════════════
# Fast path: multi-entity response building
# ═══════════════════════════════════════════════════════════════════════════════


class TestFastPathMultiEntity:
    def _fast_path(self, blobs, expected_names, grouped_refs=None, num_rows=None):
        from feast.infra.online_stores.online_store import OnlineStore

        response = GetOnlineFeaturesResponse(results=[])
        result = OnlineStore._try_precomputed_fast_path(
            blobs=blobs,
            expected_feature_names=expected_names,
            online_features_response=response,
            full_feature_names=True,
            num_rows=num_rows or len(blobs),
            grouped_refs=grouped_refs or [],
            registry=MagicMock(),
            project="test",
        )
        return result, response

    def test_multiple_entities_all_present(self):
        vec1 = _make_vector(
            feature_names=["fv1__a", "fv1__b"],
            values=[ValueProto(float_val=1.0), ValueProto(float_val=10.0)],
        )
        vec2 = _make_vector(
            feature_names=["fv1__a", "fv1__b"],
            values=[ValueProto(float_val=2.0), ValueProto(float_val=20.0)],
        )
        vec3 = _make_vector(
            feature_names=["fv1__a", "fv1__b"],
            values=[ValueProto(float_val=3.0), ValueProto(float_val=30.0)],
        )

        ok, response = self._fast_path(
            [
                vec1.SerializeToString(),
                vec2.SerializeToString(),
                vec3.SerializeToString(),
            ],
            ["fv1__a", "fv1__b"],
            num_rows=3,
        )
        assert ok is True
        assert len(response.results) == 2

        a_values = [v.float_val for v in response.results[0].values]
        b_values = [v.float_val for v in response.results[1].values]

        assert a_values == pytest.approx([1.0, 2.0, 3.0])
        assert b_values == pytest.approx([10.0, 20.0, 30.0])

    def test_statuses_are_present(self):
        vec = _make_vector(
            feature_names=["fv1__a"],
            values=[ValueProto(float_val=1.0)],
        )
        ok, response = self._fast_path(
            [vec.SerializeToString()], ["fv1__a"], num_rows=1
        )
        assert ok is True
        assert response.results[0].statuses[0] == FieldStatus.PRESENT

    def test_feature_names_in_metadata(self):
        vec = _make_vector(
            feature_names=["fv1__x", "fv2__y"],
            values=[ValueProto(float_val=1.0), ValueProto(float_val=2.0)],
        )
        ok, response = self._fast_path([vec.SerializeToString()], ["fv1__x", "fv2__y"])
        assert ok is True
        assert list(response.metadata.feature_names.val) == ["fv1__x", "fv2__y"]


# ═══════════════════════════════════════════════════════════════════════════════
# TTL enforcement in fast path
# ═══════════════════════════════════════════════════════════════════════════════


class TestFastPathTTLEnforcement:
    def _fast_path_with_ttl(self, fv_event_dt, fv_ttl_seconds, fv_name="fv1"):
        from feast.infra.online_stores.online_store import OnlineStore

        event_ts = _make_timestamp(fv_event_dt)
        fv_ts = FeatureViewTimestamp(
            feature_view_name=fv_name, event_timestamp=event_ts
        )
        vector = _make_vector(
            feature_names=[f"{fv_name}__f1"],
            values=[ValueProto(float_val=1.0)],
            fv_timestamps=[fv_ts],
        )

        fv = _make_fv(
            fv_name,
            [Field(name="f1", dtype=Float32)],
            ttl=timedelta(seconds=fv_ttl_seconds),
        )
        grouped_refs = [(fv, ["f1"])]

        response = GetOnlineFeaturesResponse(results=[])
        ok = OnlineStore._try_precomputed_fast_path(
            blobs=[vector.SerializeToString()],
            expected_feature_names=[f"{fv_name}__f1"],
            online_features_response=response,
            full_feature_names=True,
            num_rows=1,
            grouped_refs=grouped_refs,
            registry=MagicMock(),
            project="test",
        )
        return ok, response

    def test_fresh_data_is_present(self):
        now = datetime.now(tz=timezone.utc)
        ok, response = self._fast_path_with_ttl(now, fv_ttl_seconds=3600)
        assert ok is True
        assert response.results[0].statuses[0] == FieldStatus.PRESENT

    def test_expired_data_is_outside_max_age(self):
        old = datetime.now(tz=timezone.utc) - timedelta(hours=2)
        ok, response = self._fast_path_with_ttl(old, fv_ttl_seconds=3600)
        assert ok is True
        assert response.results[0].statuses[0] == FieldStatus.OUTSIDE_MAX_AGE

    def test_zero_ttl_means_no_expiry(self):
        old = datetime.now(tz=timezone.utc) - timedelta(days=365)
        fv = _make_fv(
            "fv1",
            [Field(name="f1", dtype=Float32)],
            ttl=timedelta(0),
        )

        event_ts = _make_timestamp(old)
        fv_ts = FeatureViewTimestamp(feature_view_name="fv1", event_timestamp=event_ts)
        vector = _make_vector(
            feature_names=["fv1__f1"],
            values=[ValueProto(float_val=1.0)],
            fv_timestamps=[fv_ts],
        )

        from feast.infra.online_stores.online_store import OnlineStore

        response = GetOnlineFeaturesResponse(results=[])
        ok = OnlineStore._try_precomputed_fast_path(
            blobs=[vector.SerializeToString()],
            expected_feature_names=["fv1__f1"],
            online_features_response=response,
            full_feature_names=True,
            num_rows=1,
            grouped_refs=[(fv, ["f1"])],
            registry=MagicMock(),
            project="test",
        )
        assert ok is True
        assert response.results[0].statuses[0] == FieldStatus.PRESENT

    def test_mixed_ttl_some_expired_some_fresh(self):
        from feast.infra.online_stores.online_store import OnlineStore

        now = datetime.now(tz=timezone.utc)
        fresh_ts = _make_timestamp(now)
        stale_ts = _make_timestamp(now - timedelta(hours=5))

        vector = _make_vector(
            feature_names=["fv1__f1", "fv2__f2"],
            values=[ValueProto(float_val=1.0), ValueProto(float_val=2.0)],
            fv_timestamps=[
                FeatureViewTimestamp(feature_view_name="fv1", event_timestamp=fresh_ts),
                FeatureViewTimestamp(feature_view_name="fv2", event_timestamp=stale_ts),
            ],
        )

        fv1 = _make_fv("fv1", [Field(name="f1", dtype=Float32)], ttl=timedelta(hours=1))
        fv2 = _make_fv("fv2", [Field(name="f2", dtype=Float32)], ttl=timedelta(hours=1))

        response = GetOnlineFeaturesResponse(results=[])
        ok = OnlineStore._try_precomputed_fast_path(
            blobs=[vector.SerializeToString()],
            expected_feature_names=["fv1__f1", "fv2__f2"],
            online_features_response=response,
            full_feature_names=True,
            num_rows=1,
            grouped_refs=[(fv1, ["f1"]), (fv2, ["f2"])],
            registry=MagicMock(),
            project="test",
        )
        assert ok is True
        assert response.results[0].statuses[0] == FieldStatus.PRESENT
        assert response.results[1].statuses[0] == FieldStatus.OUTSIDE_MAX_AGE


# ═══════════════════════════════════════════════════════════════════════════════
# Expected feature names computation
# ═══════════════════════════════════════════════════════════════════════════════


class TestComputeExpectedFeatureNames:
    def _compute(self, grouped_refs, full_feature_names):
        from feast.infra.online_stores.online_store import OnlineStore

        return OnlineStore._compute_expected_feature_names(
            grouped_refs, full_feature_names
        )

    def test_full_feature_names(self):
        fv = _make_fv("driver_fv", [Field(name="trips", dtype=Int64)])
        assert self._compute([(fv, ["trips"])], True) == ["driver_fv__trips"]

    def test_short_feature_names(self):
        fv = _make_fv("driver_fv", [Field(name="trips", dtype=Int64)])
        assert self._compute([(fv, ["trips"])], False) == ["trips"]

    def test_skips_ts_keys(self):
        fv = _make_fv("driver_fv", [Field(name="trips", dtype=Int64)])
        result = self._compute([(fv, ["trips", "_ts:driver_fv"])], True)
        assert result == ["driver_fv__trips"]

    def test_multiple_fvs(self):
        fv1 = _make_fv("fv1", [Field(name="a", dtype=Float32)])
        fv2 = _make_fv("fv2", [Field(name="b", dtype=Float32)])
        result = self._compute([(fv1, ["a"]), (fv2, ["b"])], True)
        assert result == ["fv1__a", "fv2__b"]

    def test_multiple_features_per_fv(self):
        fv = _make_fv(
            "fv1",
            [
                Field(name="a", dtype=Float32),
                Field(name="b", dtype=Float32),
                Field(name="c", dtype=Float32),
            ],
        )
        result = self._compute([(fv, ["a", "b", "c"])], True)
        assert result == ["fv1__a", "fv1__b", "fv1__c"]

    def test_empty_grouped_refs(self):
        assert self._compute([], True) == []

    def test_only_ts_keys(self):
        fv = _make_fv("fv1", [Field(name="a", dtype=Float32)])
        result = self._compute([(fv, ["_ts:fv1"])], True)
        assert result == []


# ═══════════════════════════════════════════════════════════════════════════════
# OnlineStore base class default implementations
# ═══════════════════════════════════════════════════════════════════════════════


class TestOnlineStoreBaseDefaults:
    def _make_dummy_store(self, online_read_fn=None, online_write_batch_fn=None):
        from feast.infra.online_stores.online_store import OnlineStore

        class DummyStore(OnlineStore):
            def __init__(self):
                self.written = []

            def online_write_batch(self, config, table, data, progress):
                self.written.append((table.name, data))
                if online_write_batch_fn:
                    online_write_batch_fn(config, table, data, progress)

            def online_read(self, config, table, entity_keys, requested_features=None):
                if online_read_fn:
                    return online_read_fn(
                        config, table, entity_keys, requested_features
                    )
                raise RuntimeError("not implemented")

            def update(self, *args, **kwargs):
                pass

            def teardown(self, *args, **kwargs):
                pass

        return DummyStore()

    def test_read_precomputed_returns_none_on_exception(self):
        store = self._make_dummy_store()
        config = MagicMock()
        entity_keys = [MagicMock(), MagicMock()]

        result = store.read_precomputed_vectors(
            config, "my_service", "project", entity_keys
        )
        assert result == [None, None]

    def test_read_precomputed_returns_none_for_missing_entities(self):
        def online_read_fn(config, table, entity_keys, requested_features):
            return [(None, None) for _ in entity_keys]

        store = self._make_dummy_store(online_read_fn=online_read_fn)
        result = store.read_precomputed_vectors(
            MagicMock(), "svc", "proj", [MagicMock()]
        )
        assert result == [None]

    def test_read_precomputed_returns_bytes_for_present_entities(self):
        test_bytes = b"test_vector_data"

        def online_read_fn(config, table, entity_keys, requested_features):
            return [
                (
                    datetime.now(tz=timezone.utc),
                    {"vector": ValueProto(bytes_val=test_bytes)},
                )
                for _ in entity_keys
            ]

        store = self._make_dummy_store(online_read_fn=online_read_fn)
        result = store.read_precomputed_vectors(
            MagicMock(), "svc", "proj", [MagicMock()]
        )
        assert result == [test_bytes]

    def test_write_precomputed_delegates_to_online_write_batch(self):
        store = self._make_dummy_store()
        entity_key = MagicMock()
        vector_bytes = b"blob_data"

        store.write_precomputed_vector(
            MagicMock(), "my_svc", "proj", entity_key, vector_bytes
        )
        assert len(store.written) == 1
        table_name, data = store.written[0]
        assert table_name == "__precomputed__my_svc"
        assert len(data) == 1
        assert data[0][1]["vector"].bytes_val == vector_bytes

    def test_write_then_read_roundtrip(self):
        stored = {}

        def online_write_fn(config, table, data, progress):
            for ek, vals, ts, _ in data:
                stored[id(ek)] = (ts, vals)

        def online_read_fn(config, table, entity_keys, requested_features):
            results = []
            for ek in entity_keys:
                if id(ek) in stored:
                    ts, vals = stored[id(ek)]
                    results.append((ts, vals))
                else:
                    results.append((None, None))
            return results

        store = self._make_dummy_store(
            online_read_fn=online_read_fn,
            online_write_batch_fn=online_write_fn,
        )

        entity_key = MagicMock()
        vector_bytes = b"test_roundtrip"

        store.write_precomputed_vector(
            MagicMock(), "svc", "proj", entity_key, vector_bytes
        )
        result = store.read_precomputed_vectors(
            MagicMock(), "svc", "proj", [entity_key]
        )
        assert result == [vector_bytes]

    def test_read_returns_none_for_non_bytes_val(self):
        def online_read_fn(config, table, entity_keys, requested_features):
            return [
                (
                    datetime.now(tz=timezone.utc),
                    {"vector": ValueProto(string_val="not bytes")},
                )
                for _ in entity_keys
            ]

        store = self._make_dummy_store(online_read_fn=online_read_fn)
        result = store.read_precomputed_vectors(
            MagicMock(), "svc", "proj", [MagicMock()]
        )
        assert result == [None]

    def test_read_returns_none_when_no_vector_key(self):
        def online_read_fn(config, table, entity_keys, requested_features):
            return [
                (
                    datetime.now(tz=timezone.utc),
                    {"other_field": ValueProto(float_val=1.0)},
                )
                for _ in entity_keys
            ]

        store = self._make_dummy_store(online_read_fn=online_read_fn)
        result = store.read_precomputed_vectors(
            MagicMock(), "svc", "proj", [MagicMock()]
        )
        assert result == [None]


# ═══════════════════════════════════════════════════════════════════════════════
# Materialization hook: _precompute_affected_services
# ═══════════════════════════════════════════════════════════════════════════════


class TestPrecomputeAffectedServices:
    def _make_mock_store(self, services):
        store = MagicMock()
        store.registry.list_feature_services.return_value = services
        store.project = "test_project"
        store._precompute_affected_services = lambda fv_names: type(
            store
        )._precompute_affected_services(store, fv_names)

        from feast.feature_store import FeatureStore

        store._precompute_affected_services = (
            FeatureStore._precompute_affected_services.__get__(store)
        )
        return store

    def test_triggers_precompute_for_matching_service(self):
        proj = MagicMock()
        proj.name = "fv1"
        svc = MagicMock()
        svc.precompute_online = True
        svc.feature_view_projections = [proj]
        svc.name = "my_service"

        store = self._make_mock_store([svc])
        store._precompute_affected_services(["fv1"])
        store.precompute_feature_service.assert_called_once_with("my_service")

    def test_skips_non_precompute_services(self):
        svc = MagicMock()
        svc.precompute_online = False
        svc.name = "svc"

        store = self._make_mock_store([svc])
        store._precompute_affected_services(["fv1"])
        store.precompute_feature_service.assert_not_called()

    def test_skips_when_no_fvs_match(self):
        proj = MagicMock()
        proj.name = "fv_other"
        svc = MagicMock()
        svc.precompute_online = True
        svc.feature_view_projections = [proj]
        svc.name = "svc"

        store = self._make_mock_store([svc])
        store._precompute_affected_services(["fv1"])
        store.precompute_feature_service.assert_not_called()

    def test_handles_registry_exception_gracefully(self):
        store = MagicMock()
        store.registry.list_feature_services.side_effect = RuntimeError("oops")
        store.project = "test"

        from feast.feature_store import FeatureStore

        FeatureStore._precompute_affected_services(store, ["fv1"])

    def test_handles_precompute_exception_gracefully(self):
        proj = MagicMock()
        proj.name = "fv1"
        svc = MagicMock()
        svc.precompute_online = True
        svc.feature_view_projections = [proj]
        svc.name = "svc"

        store = self._make_mock_store([svc])
        store.precompute_feature_service.side_effect = RuntimeError("fail")

        store._precompute_affected_services(["fv1"])

    def test_multiple_services_all_triggered(self):
        proj1 = MagicMock()
        proj1.name = "fv1"
        svc1 = MagicMock()
        svc1.precompute_online = True
        svc1.feature_view_projections = [proj1]
        svc1.name = "svc1"

        proj2 = MagicMock()
        proj2.name = "fv1"
        svc2 = MagicMock()
        svc2.precompute_online = True
        svc2.feature_view_projections = [proj2]
        svc2.name = "svc2"

        store = self._make_mock_store([svc1, svc2])
        store._precompute_affected_services(["fv1"])

        assert store.precompute_feature_service.call_count == 2


# ═══════════════════════════════════════════════════════════════════════════════
# Push hook: _precompute_for_push
# ═══════════════════════════════════════════════════════════════════════════════


class TestPrecomputeForPush:
    def _make_mock_store(self, services):
        store = MagicMock()
        store.registry.list_feature_services.return_value = services
        store.project = "test_project"

        from feast.feature_store import FeatureStore

        store._precompute_for_push = FeatureStore._precompute_for_push.__get__(store)
        return store

    def test_triggers_for_matching_fv(self):
        proj = MagicMock()
        proj.name = "pushed_fv"
        svc = MagicMock()
        svc.precompute_online = True
        svc.feature_view_projections = [proj]
        svc.name = "svc"

        store = self._make_mock_store([svc])
        store._precompute_for_push("pushed_fv", MagicMock())
        store.precompute_feature_service.assert_called_once_with("svc")

    def test_skips_non_matching_fv(self):
        proj = MagicMock()
        proj.name = "other_fv"
        svc = MagicMock()
        svc.precompute_online = True
        svc.feature_view_projections = [proj]
        svc.name = "svc"

        store = self._make_mock_store([svc])
        store._precompute_for_push("pushed_fv", MagicMock())
        store.precompute_feature_service.assert_not_called()

    def test_skips_non_precompute_service(self):
        proj = MagicMock()
        proj.name = "pushed_fv"
        svc = MagicMock()
        svc.precompute_online = False
        svc.feature_view_projections = [proj]
        svc.name = "svc"

        store = self._make_mock_store([svc])
        store._precompute_for_push("pushed_fv", MagicMock())
        store.precompute_feature_service.assert_not_called()

    def test_handles_exception_gracefully(self):
        proj = MagicMock()
        proj.name = "fv"
        svc = MagicMock()
        svc.precompute_online = True
        svc.feature_view_projections = [proj]
        svc.name = "svc"

        store = self._make_mock_store([svc])
        store.precompute_feature_service.side_effect = RuntimeError("fail")

        store._precompute_for_push("fv", MagicMock())

    def test_handles_registry_exception_gracefully(self):
        store = MagicMock()
        store.registry.list_feature_services.side_effect = RuntimeError("oops")
        store.project = "test"

        from feast.feature_store import FeatureStore

        FeatureStore._precompute_for_push(store, "fv", MagicMock())
