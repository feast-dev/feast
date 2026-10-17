"""Unit tests for Datastore online store feature view versioning."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("google.cloud.datastore")

from google.cloud.datastore.key import Key  # noqa: E402

from feast import Entity, FeatureView  # noqa: E402
from feast.field import Field  # noqa: E402
from feast.infra.online_stores.datastore import (  # noqa: E402
    DatastoreOnlineStore,
    DatastoreOnlineStoreConfig,
    _table_id,
)
from feast.protos.feast.types.EntityKey_pb2 import (  # noqa: E402
    EntityKey as EntityKeyProto,
)
from feast.protos.feast.types.Value_pb2 import Value as ValueProto  # noqa: E402
from feast.types import Float32  # noqa: E402
from feast.value_type import ValueType  # noqa: E402


def _make_feature_view(name="driver_stats", version_number=None, version_tag=None):
    entity = Entity(
        name="driver_id",
        join_keys=["driver_id"],
        value_type=ValueType.INT64,
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
    config.online_store = DatastoreOnlineStoreConfig(
        write_concurrency=1,
        write_batch_size=50,
    )
    config.registry.enable_online_feature_view_versioning = versioning
    return config


def _make_entity_key(driver_id=1):
    return EntityKeyProto(
        join_keys=["driver_id"],
        entity_values=[ValueProto(int64_val=driver_id)],
    )


def _make_mock_client():
    client = MagicMock()
    client.key.side_effect = lambda *args: Key(*args, project="test-gcp-project")
    return client


class TestDatastoreTableId:
    """Test _table_id produces correct versioned table names."""

    def test_default_no_versioning(self):
        fv = _make_feature_view()
        assert _table_id(fv) == "driver_stats"

    def test_versioning_explicitly_disabled(self):
        fv = _make_feature_view(version_number=3)
        assert _table_id(fv, enable_versioning=False) == "driver_stats"

    def test_versioning_enabled_no_version_set(self):
        fv = _make_feature_view()
        assert _table_id(fv, enable_versioning=True) == "driver_stats"

    def test_versioning_enabled_with_current_version_number(self):
        fv = _make_feature_view(version_number=2)
        assert _table_id(fv, enable_versioning=True) == "driver_stats_v2"

    def test_version_zero_no_suffix(self):
        fv = _make_feature_view(version_number=0)
        assert _table_id(fv, enable_versioning=True) == "driver_stats"

    def test_projection_version_tag_takes_priority(self):
        fv = _make_feature_view(version_number=1, version_tag=3)
        assert _table_id(fv, enable_versioning=True) == "driver_stats_v3"

    def test_projection_version_tag_zero_no_suffix(self):
        fv = _make_feature_view(version_tag=0, version_number=3)
        assert _table_id(fv, enable_versioning=True) == "driver_stats"


class TestDatastoreVersionedReadSupport:
    """Test that DatastoreOnlineStore passes _check_versioned_read_support."""

    def test_is_versioned_read_supported(self):
        store = DatastoreOnlineStore()
        assert store._is_versioned_read_supported() is True

    def test_allowed_with_version_tag(self):
        store = DatastoreOnlineStore()
        fv = _make_feature_view()
        fv.projection.version_tag = 2
        store._check_versioned_read_support([(fv, ["trips_today"])])

    def test_allowed_without_version_tag(self):
        store = DatastoreOnlineStore()
        fv = _make_feature_view()
        store._check_versioned_read_support([(fv, ["trips_today"])])


class TestDatastoreOnlineStoreVersionedOperations:
    """Test that DatastoreOnlineStore read/write/update/teardown use versioned table keys."""

    def test_online_write_batch_uses_versioned_table_key(self):
        store = DatastoreOnlineStore()
        client = _make_mock_client()
        store._client = client
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=2)
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)

        store.online_write_batch(
            config,
            fv,
            [(_make_entity_key(1), {"trips_today": ValueProto(float_val=5.0)}, ts, ts)],
            None,
        )

        assert client.key.call_args.args[:4] == (
            "Project",
            "test_project",
            "Table",
            "driver_stats_v2",
        )

    def test_online_write_batch_unversioned_when_disabled(self):
        store = DatastoreOnlineStore()
        client = _make_mock_client()
        store._client = client
        config = _make_config(versioning=False)
        fv = _make_feature_view(version_number=2)
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)

        store.online_write_batch(
            config,
            fv,
            [(_make_entity_key(1), {"trips_today": ValueProto(float_val=5.0)}, ts, ts)],
            None,
        )

        assert client.key.call_args.args[:4] == (
            "Project",
            "test_project",
            "Table",
            "driver_stats",
        )

    def test_online_read_uses_versioned_table_key(self):
        store = DatastoreOnlineStore()
        client = _make_mock_client()
        client.get_multi.return_value = []
        store._client = client
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=2)

        results = store.online_read(config, fv, [_make_entity_key(1)], ["trips_today"])

        assert results == [(None, None)]
        assert client.key.call_args.args[:4] == (
            "Project",
            "test_project",
            "Table",
            "driver_stats_v2",
        )

    def test_online_read_uses_projection_version_tag(self):
        store = DatastoreOnlineStore()
        client = _make_mock_client()
        client.get_multi.return_value = []
        store._client = client
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=1, version_tag=3)

        store.online_read(config, fv, [_make_entity_key(1)], ["trips_today"])

        assert client.key.call_args.args[:4] == (
            "Project",
            "test_project",
            "Table",
            "driver_stats_v3",
        )

    def test_update_uses_versioned_table_keys(self):
        store = DatastoreOnlineStore()
        client = _make_mock_client()
        store._client = client
        config = _make_config(versioning=True)
        fv_keep = _make_feature_view(name="keep_fv", version_number=2)
        fv_delete = _make_feature_view(name="delete_fv", version_number=3)

        with patch(
            "feast.infra.online_stores.datastore._delete_all_values"
        ) as mock_delete_vals:
            store.update(
                config=config,
                tables_to_delete=[fv_delete],
                tables_to_keep=[fv_keep],
                entities_to_delete=[],
                entities_to_keep=[],
                partial=False,
            )

        put_entity = client.put.call_args.args[0]
        assert put_entity.key.flat_path == (
            "Project",
            "test_project",
            "Table",
            "keep_fv_v2",
        )
        deleted_val_key = mock_delete_vals.call_args.args[1]
        assert deleted_val_key.flat_path == (
            "Project",
            "test_project",
            "Table",
            "delete_fv_v3",
        )
        deleted_table_key = client.delete.call_args.args[0]
        assert deleted_table_key.flat_path == (
            "Project",
            "test_project",
            "Table",
            "delete_fv_v3",
        )

    def test_teardown_uses_versioned_table_keys(self):
        store = DatastoreOnlineStore()
        client = _make_mock_client()
        store._client = client
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=2)

        with patch(
            "feast.infra.online_stores.datastore._delete_all_values"
        ) as mock_delete_vals:
            store.teardown(config, [fv], [])

        deleted_val_key = mock_delete_vals.call_args.args[1]
        assert deleted_val_key.flat_path == (
            "Project",
            "test_project",
            "Table",
            "driver_stats_v2",
        )
        deleted_table_key = client.delete.call_args.args[0]
        assert deleted_table_key.flat_path == (
            "Project",
            "test_project",
            "Table",
            "driver_stats_v2",
        )
