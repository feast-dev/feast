import copy
from datetime import datetime, timedelta
from tempfile import mkstemp

import pytest

from feast.data_format import AvroFormat, ParquetFormat
from feast.data_source import KafkaSource
from feast.entity import Entity
from feast.feature_store import FeatureStore
from feast.feature_view import FeatureView, FeatureViewState
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.infra.online_stores.sqlite import SqliteOnlineStoreConfig
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.protos.feast.core.FeatureView_pb2 import FeatureView as FeatureViewProto
from feast.protos.feast.core.FeatureView_pb2 import (
    FeatureViewMeta as FeatureViewMetaProto,
)
from feast.protos.feast.core.FeatureView_pb2 import (
    FeatureViewSpec as FeatureViewSpecProto,
)
from feast.repo_config import RepoConfig
from feast.stream_feature_view import StreamFeatureView
from feast.types import Float32, Int64

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _kafka_source():
    return KafkaSource(
        name="kafka",
        timestamp_field="event_timestamp",
        kafka_bootstrap_servers="localhost:9092",
        message_format=AvroFormat(""),
        topic="topic",
        batch_source=_batch_source(),
    )


def _batch_source():
    return FileSource(
        file_format=ParquetFormat(),
        path="file://feast/*",
        timestamp_field="ts_col",
        created_timestamp_column="timestamp",
    )


def _simple_feature_view(name="test_fv", enabled=True):
    return FeatureView(
        name=name,
        entities=[],
        schema=[Field(name="f1", dtype=Float32)],
        source=_batch_source(),
        ttl=timedelta(days=1),
        enabled=enabled,
    )


@pytest.fixture
def local_feature_store():
    _, registry_path = mkstemp()
    _, online_store_path = mkstemp()
    return FeatureStore(
        config=RepoConfig(
            registry=registry_path,
            project="default",
            provider="local",
            online_store=SqliteOnlineStoreConfig(path=online_store_path),
            entity_key_serialization_version=3,
        )
    )


# ---------------------------------------------------------------------------
# FeatureViewState enum
# ---------------------------------------------------------------------------


class TestFeatureViewState:
    def test_state_values(self):
        assert FeatureViewState.STATE_UNSPECIFIED == 0
        assert FeatureViewState.CREATED == 1
        assert FeatureViewState.GENERATED == 2
        assert FeatureViewState.MATERIALIZING == 3
        assert FeatureViewState.AVAILABLE_ONLINE == 4

    def test_from_proto_valid(self):
        assert FeatureViewState.from_proto(0) == FeatureViewState.STATE_UNSPECIFIED
        assert FeatureViewState.from_proto(4) == FeatureViewState.AVAILABLE_ONLINE

    def test_from_proto_invalid_falls_back(self):
        assert FeatureViewState.from_proto(999) == FeatureViewState.STATE_UNSPECIFIED

    def test_to_proto_round_trip(self):
        for state in FeatureViewState:
            assert FeatureViewState.from_proto(state.to_proto()) == state

    def test_valid_transitions(self):
        """Verify that valid transitions are accepted."""
        assert FeatureViewState.STATE_UNSPECIFIED.can_transition_to(
            FeatureViewState.CREATED
        )
        assert FeatureViewState.CREATED.can_transition_to(FeatureViewState.GENERATED)
        assert FeatureViewState.GENERATED.can_transition_to(
            FeatureViewState.MATERIALIZING
        )
        assert FeatureViewState.MATERIALIZING.can_transition_to(
            FeatureViewState.AVAILABLE_ONLINE
        )
        assert FeatureViewState.MATERIALIZING.can_transition_to(
            FeatureViewState.GENERATED
        )
        assert FeatureViewState.AVAILABLE_ONLINE.can_transition_to(
            FeatureViewState.MATERIALIZING
        )

    def test_invalid_transitions(self):
        """Verify that invalid transitions are rejected."""
        assert not FeatureViewState.STATE_UNSPECIFIED.can_transition_to(
            FeatureViewState.AVAILABLE_ONLINE
        )
        assert not FeatureViewState.CREATED.can_transition_to(
            FeatureViewState.MATERIALIZING
        )
        assert not FeatureViewState.CREATED.can_transition_to(
            FeatureViewState.AVAILABLE_ONLINE
        )
        assert not FeatureViewState.GENERATED.can_transition_to(
            FeatureViewState.AVAILABLE_ONLINE
        )
        assert not FeatureViewState.AVAILABLE_ONLINE.can_transition_to(
            FeatureViewState.CREATED
        )


# ---------------------------------------------------------------------------
# FeatureView enabled / state defaults
# ---------------------------------------------------------------------------


class TestFeatureViewDefaults:
    def test_default_enabled_is_true(self):
        fv = _simple_feature_view()
        assert fv.enabled is True

    def test_default_state_is_unspecified(self):
        fv = _simple_feature_view()
        assert fv.state == FeatureViewState.STATE_UNSPECIFIED

    def test_enabled_false(self):
        fv = _simple_feature_view(enabled=False)
        assert fv.enabled is False


# ---------------------------------------------------------------------------
# Proto serialization round-trips
# ---------------------------------------------------------------------------


class TestFeatureViewProtoRoundTrip:
    def test_enabled_true_round_trip(self):
        fv = _simple_feature_view(enabled=True)
        proto = fv.to_proto()
        assert proto.spec.disabled is False
        restored = FeatureView.from_proto(proto)
        assert restored.enabled is True

    def test_enabled_false_round_trip(self):
        fv = _simple_feature_view(enabled=False)
        proto = fv.to_proto()
        assert proto.spec.disabled is True
        restored = FeatureView.from_proto(proto)
        assert restored.enabled is False

    def test_state_round_trip(self):
        fv = _simple_feature_view()
        fv.state = FeatureViewState.AVAILABLE_ONLINE
        proto = fv.to_proto()
        assert proto.meta.state == FeatureViewState.AVAILABLE_ONLINE.value
        restored = FeatureView.from_proto(proto)
        assert restored.state == FeatureViewState.AVAILABLE_ONLINE

    def test_state_unspecified_not_written_to_proto(self):
        fv = _simple_feature_view()
        assert fv.state == FeatureViewState.STATE_UNSPECIFIED
        proto = fv.to_proto()
        assert proto.meta.state == 0

    def test_backward_compat_old_proto_without_disabled_field(self):
        """Old protos without `disabled` field default to False -> enabled=True."""
        spec = FeatureViewSpecProto()
        spec.name = "legacy_fv"
        proto = FeatureViewProto(spec=spec, meta=FeatureViewMetaProto())
        fv = FeatureView.from_proto(proto)
        assert fv.enabled is True

    def test_backward_compat_old_proto_without_state_field(self):
        """Old protos without `state` field default to 0 -> STATE_UNSPECIFIED."""
        spec = FeatureViewSpecProto()
        spec.name = "legacy_fv"
        proto = FeatureViewProto(spec=spec, meta=FeatureViewMetaProto())
        fv = FeatureView.from_proto(proto)
        assert fv.state == FeatureViewState.STATE_UNSPECIFIED

    def test_all_states_round_trip(self):
        for state in FeatureViewState:
            fv = _simple_feature_view()
            fv.state = state
            restored = FeatureView.from_proto(fv.to_proto())
            assert restored.state == state


# ---------------------------------------------------------------------------
# copy.copy preserves enabled/state
# ---------------------------------------------------------------------------


class TestCopyPreservesState:
    def test_feature_view_copy(self):
        fv = _simple_feature_view(enabled=False)
        fv.state = FeatureViewState.GENERATED
        copied = copy.copy(fv)
        assert copied.enabled is False
        assert copied.state == FeatureViewState.GENERATED

    def test_on_demand_feature_view_copy(self):
        source_fv = _simple_feature_view()
        odfv = OnDemandFeatureView(
            name="test_odfv",
            sources=[source_fv],
            schema=[Field(name="out", dtype=Float32)],
            mode="python",
            udf=lambda features: {"out": [1.0]},
            enabled=False,
        )
        odfv.state = FeatureViewState.GENERATED
        copied = copy.copy(odfv)
        assert copied.enabled is False
        assert copied.state == FeatureViewState.GENERATED

    def test_stream_feature_view_copy(self):
        sfv = StreamFeatureView(
            name="test_sfv",
            entities=[],
            schema=[Field(name="f1", dtype=Float32)],
            source=_kafka_source(),
            ttl=timedelta(days=1),
        )
        sfv.enabled = False
        sfv.state = FeatureViewState.AVAILABLE_ONLINE
        copied = copy.copy(sfv)
        assert copied.enabled is False
        assert copied.state == FeatureViewState.AVAILABLE_ONLINE


# ---------------------------------------------------------------------------
# OnDemandFeatureView enabled / state
# ---------------------------------------------------------------------------


class TestOnDemandFeatureViewState:
    def test_default_enabled(self):
        source_fv = _simple_feature_view()
        odfv = OnDemandFeatureView(
            name="test_odfv",
            sources=[source_fv],
            schema=[Field(name="out", dtype=Float32)],
            mode="python",
            udf=lambda features: {"out": [1.0]},
        )
        assert odfv.enabled is True
        assert odfv.state == FeatureViewState.STATE_UNSPECIFIED

    def test_disabled(self):
        source_fv = _simple_feature_view()
        odfv = OnDemandFeatureView(
            name="test_odfv",
            sources=[source_fv],
            schema=[Field(name="out", dtype=Float32)],
            mode="python",
            udf=lambda features: {"out": [1.0]},
            enabled=False,
        )
        assert odfv.enabled is False

    def test_proto_disabled_field(self):
        """Verify the proto disabled field is set correctly without full round-trip."""
        source_fv = _simple_feature_view()
        odfv = OnDemandFeatureView(
            name="test_odfv",
            sources=[source_fv],
            schema=[Field(name="out", dtype=Float32)],
            mode="python",
            udf=lambda features: {"out": [1.0]},
            enabled=False,
        )
        odfv.state = FeatureViewState.AVAILABLE_ONLINE
        proto = odfv.to_proto()
        assert proto.spec.disabled is True
        assert proto.meta.state == FeatureViewState.AVAILABLE_ONLINE.value


# ---------------------------------------------------------------------------
# StreamFeatureView enabled / state
# ---------------------------------------------------------------------------


class TestStreamFeatureViewState:
    def test_proto_round_trip(self):
        sfv = StreamFeatureView(
            name="test_sfv",
            entities=[],
            schema=[Field(name="f1", dtype=Float32)],
            source=_kafka_source(),
            ttl=timedelta(days=1),
        )
        sfv.enabled = False
        sfv.state = FeatureViewState.MATERIALIZING
        proto = sfv.to_proto()
        assert proto.spec.disabled is True
        restored = StreamFeatureView.from_proto(proto)
        assert restored.enabled is False
        assert restored.state == FeatureViewState.MATERIALIZING


# ---------------------------------------------------------------------------
# Registry apply preserves enabled/state
# ---------------------------------------------------------------------------


class TestRegistryEnabledState:
    def test_apply_and_retrieve_enabled(self, local_feature_store):
        store = local_feature_store
        fv = _simple_feature_view(enabled=True)
        store.apply([fv])
        retrieved = store.get_feature_view("test_fv")
        assert retrieved.enabled is True
        store.teardown()

    def test_apply_and_retrieve_disabled(self, local_feature_store):
        store = local_feature_store
        fv = _simple_feature_view(enabled=False)
        store.apply([fv])
        retrieved = store.get_feature_view("test_fv")
        assert retrieved.enabled is False
        store.teardown()

    def test_toggle_enabled_via_registry(self, local_feature_store):
        store = local_feature_store
        fv = _simple_feature_view(enabled=True)
        store.apply([fv])

        # Disable it
        fv.enabled = False
        store.registry.apply_feature_view(fv, store.project)
        retrieved = store.get_feature_view("test_fv")
        assert retrieved.enabled is False

        # Re-enable it
        fv.enabled = True
        store.registry.apply_feature_view(fv, store.project)
        retrieved = store.get_feature_view("test_fv")
        assert retrieved.enabled is True
        store.teardown()

    def test_state_persists_through_registry(self, local_feature_store):
        store = local_feature_store
        fv = _simple_feature_view()
        fv.state = FeatureViewState.GENERATED
        store.apply([fv])

        # State should be updated via registry apply
        fv.state = FeatureViewState.AVAILABLE_ONLINE
        store.registry.apply_feature_view(fv, store.project)
        retrieved = store.get_feature_view("test_fv")
        assert retrieved.state == FeatureViewState.AVAILABLE_ONLINE
        store.teardown()

    def test_reapply_does_not_reset_state(self, local_feature_store):
        """feast apply with a default-state FV must not reset an existing state."""
        store = local_feature_store
        fv = _simple_feature_view()
        store.apply([fv])

        # Simulate materialization having moved state to AVAILABLE_ONLINE
        fv.state = FeatureViewState.AVAILABLE_ONLINE
        store.registry.apply_feature_view(fv, store.project)
        retrieved = store.get_feature_view("test_fv")
        assert retrieved.state == FeatureViewState.AVAILABLE_ONLINE

        # Re-apply with a fresh FV (default STATE_UNSPECIFIED) — should preserve state
        fresh_fv = _simple_feature_view()
        assert fresh_fv.state == FeatureViewState.STATE_UNSPECIFIED
        store.apply([fresh_fv])
        retrieved = store.get_feature_view("test_fv")
        assert retrieved.state == FeatureViewState.AVAILABLE_ONLINE
        store.teardown()


# ---------------------------------------------------------------------------
# Materialization blocks disabled feature views
# ---------------------------------------------------------------------------


class TestMaterializationDisabledBlocking:
    def test_materialize_disabled_fv_by_name_raises(self, local_feature_store):
        store = local_feature_store
        entity = Entity(name="entity_1", join_keys=["entity_id"])
        fv = FeatureView(
            name="test_fv",
            entities=[entity],
            schema=[
                Field(name="f1", dtype=Float32),
                Field(name="entity_id", dtype=Int64),
            ],
            source=_batch_source(),
            ttl=timedelta(days=1),
            online=True,
            enabled=False,
        )
        store.apply([entity, fv])

        with pytest.raises(ValueError, match="disabled"):
            store.materialize(
                feature_views=["test_fv"],
                start_date=datetime.utcnow() - timedelta(hours=1),
                end_date=datetime.utcnow(),
            )
        store.teardown()
