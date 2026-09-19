"""Unit tests for Hazelcast online store feature view versioning."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from feast import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Float32
from feast.value_type import ValueType

pytest.importorskip("hazelcast")

from feast.infra.online_stores.hazelcast_online_store.hazelcast_online_store import (  # noqa: E402
    HazelcastOnlineStore,
    HazelcastOnlineStoreConfig,
    _map_name,
)


def _make_feature_view(name="driver_stats", version_number=None, version_tag=None):
    entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    fv = FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name="trips_today", dtype=Float32)],
    )
    if version_number is not None:
        fv.current_version_number = version_number
    if version_tag is not None:
        fv.projection.version_tag = version_tag
    return fv


def _make_config(project="test_project", versioning=False):
    config = MagicMock()
    config.project = project
    config.entity_key_serialization_version = 3
    config.online_store = HazelcastOnlineStoreConfig(type="hazelcast")
    config.registry.enable_online_feature_view_versioning = versioning
    return config


class TestMapName:
    def test_no_versioning(self):
        assert (
            _map_name("test_project", _make_feature_view())
            == "test_project_driver_stats"
        )

    def test_versioning_disabled_ignores_version(self):
        fv = _make_feature_view(version_number=2)
        assert _map_name("test_project", fv, False) == "test_project_driver_stats"

    def test_versioning_enabled_with_version(self):
        fv = _make_feature_view(version_number=2)
        assert _map_name("test_project", fv, True) == "test_project_driver_stats_v2"

    def test_projection_version_tag_takes_priority(self):
        fv = _make_feature_view(version_number=1, version_tag=3)
        assert _map_name("test_project", fv, True) == "test_project_driver_stats_v3"

    def test_version_zero_no_suffix(self):
        fv = _make_feature_view(version_number=0)
        assert _map_name("test_project", fv, True) == "test_project_driver_stats"

    def test_versions_do_not_collide(self):
        v1 = _map_name("p", _make_feature_view(version_number=1), True)
        v2 = _map_name("p", _make_feature_view(version_number=2), True)
        assert v1 != v2


@pytest.fixture
def store_with_mock_client():
    store = HazelcastOnlineStore()
    client = MagicMock()
    with patch.object(HazelcastOnlineStore, "_get_client", return_value=client):
        yield store, client


class TestVersionedMapNamesReachHazelcast:
    @pytest.mark.parametrize(
        "versioning, expected",
        [(False, "test_project_driver_stats"), (True, "test_project_driver_stats_v2")],
    )
    def test_write_targets_the_versioned_map(
        self, store_with_mock_client, versioning, expected
    ):
        store, client = store_with_mock_client
        fv = _make_feature_view(version_number=2)
        entity_key = EntityKeyProto(
            join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1)]
        )

        store.online_write_batch(
            _make_config(versioning=versioning),
            fv,
            [
                (
                    entity_key,
                    {"trips_today": ValueProto(float_val=1.0)},
                    datetime(2024, 1, 1, tzinfo=timezone.utc),
                    None,
                )
            ],
            None,
        )

        client.get_map.assert_called_once_with(expected)

    def test_read_targets_the_versioned_map(self, store_with_mock_client):
        store, client = store_with_mock_client
        client.get_map.return_value.get_all.return_value.result.return_value = {}
        fv = _make_feature_view(version_number=2)
        entity_key = EntityKeyProto(
            join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1)]
        )

        store.online_read(
            _make_config(versioning=True), fv, [entity_key], ["trips_today"]
        )

        client.get_map.assert_called_once_with("test_project_driver_stats_v2")

    def test_update_creates_the_versioned_mapping(self, store_with_mock_client):
        store, client = store_with_mock_client
        fv = _make_feature_view(version_number=2)

        store.update(_make_config(versioning=True), [], [fv], [], [], partial=False)

        sql = client.sql.execute.call_args[0][0]
        assert "CREATE OR REPLACE MAPPING test_project_driver_stats_v2 (" in sql

    def test_update_drops_the_versioned_mapping(self, store_with_mock_client):
        store, client = store_with_mock_client
        fv = _make_feature_view(name="old_stats", version_number=1)

        store.update(_make_config(versioning=True), [fv], [], [], [], partial=False)

        statements = [c[0][0] for c in client.sql.execute.call_args_list]
        assert "DELETE FROM test_project_old_stats_v1" in statements
        assert "DROP MAPPING IF EXISTS test_project_old_stats_v1" in statements

    def test_teardown_drops_the_versioned_mapping(self, store_with_mock_client):
        store, client = store_with_mock_client
        fv = _make_feature_view(version_number=2)

        store.teardown(_make_config(versioning=True), [fv], [])

        statements = [c[0][0] for c in client.sql.execute.call_args_list]
        assert "DELETE FROM test_project_driver_stats_v2" in statements
        assert "DROP MAPPING IF EXISTS test_project_driver_stats_v2" in statements


class TestVersionedReadSupport:
    def test_hazelcast_is_allowlisted(self):
        """online_read yields one aligned entry per requested key, so this is honest."""
        store = HazelcastOnlineStore()
        store._versioned_read_supported = None
        assert store._is_versioned_read_supported() is True

    def test_versioned_ref_does_not_raise(self):
        store = HazelcastOnlineStore()
        store._versioned_read_supported = None
        fv = _make_feature_view(version_tag=2)
        store._check_versioned_read_support([(fv, ["trips_today"])])

    def test_read_returns_one_entry_per_requested_key(self, store_with_mock_client):
        """The property the allowlist actually depends on: misses yield (None, None)."""
        store, client = store_with_mock_client
        client.get_map.return_value.get_all.return_value.result.return_value = {}
        fv = _make_feature_view(version_number=2)
        entity_keys = [
            EntityKeyProto(
                join_keys=["driver_id"], entity_values=[ValueProto(int64_val=i)]
            )
            for i in (1, 2, 3)
        ]

        result = store.online_read(
            _make_config(versioning=True), fv, entity_keys, ["trips_today"]
        )

        assert len(result) == len(entity_keys)
        assert all(entry == (None, None) for entry in result)
