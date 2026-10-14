"""Unit tests for Bigtable online store feature view versioning."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("google.cloud.bigtable")

from feast import Entity, FeatureView  # noqa: E402
from feast.field import Field  # noqa: E402
from feast.infra.online_stores.bigtable import (  # noqa: E402
    BigtableOnlineStore,
    BigtableOnlineStoreConfig,
)
from feast.infra.online_stores.helpers import compute_entity_id  # noqa: E402
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
    config.online_store = BigtableOnlineStoreConfig(
        instance="test-instance",
        project_id="test-gcp-project",
        write_concurrency=1,
    )
    config.registry.enable_online_feature_view_versioning = versioning
    return config


def _make_entity_key(driver_id=1):
    return EntityKeyProto(
        join_keys=["driver_id"],
        entity_values=[ValueProto(int64_val=driver_id)],
    )


class TestVersionedFvName:
    """Test _versioned_fv_name produces correct versioned names."""

    def test_no_versioning(self):
        from feast.infra.online_stores.bigtable import _versioned_fv_name

        fv = _make_feature_view()
        config = _make_config(versioning=False)
        assert _versioned_fv_name(fv, config) == "driver_stats"

    def test_versioning_disabled_ignores_version(self):
        from feast.infra.online_stores.bigtable import _versioned_fv_name

        fv = _make_feature_view(version_number=3)
        config = _make_config(versioning=False)
        assert _versioned_fv_name(fv, config) == "driver_stats"

    def test_versioning_enabled_no_version_set(self):
        from feast.infra.online_stores.bigtable import _versioned_fv_name

        fv = _make_feature_view()
        config = _make_config(versioning=True)
        assert _versioned_fv_name(fv, config) == "driver_stats"

    def test_versioning_enabled_with_current_version_number(self):
        from feast.infra.online_stores.bigtable import _versioned_fv_name

        fv = _make_feature_view(version_number=2)
        config = _make_config(versioning=True)
        assert _versioned_fv_name(fv, config) == "driver_stats_v2"

    def test_version_zero_no_suffix(self):
        from feast.infra.online_stores.bigtable import _versioned_fv_name

        fv = _make_feature_view(version_number=0)
        config = _make_config(versioning=True)
        assert _versioned_fv_name(fv, config) == "driver_stats"

    def test_projection_version_tag_takes_priority(self):
        from feast.infra.online_stores.bigtable import _versioned_fv_name

        fv = _make_feature_view(version_number=1, version_tag=3)
        config = _make_config(versioning=True)
        assert _versioned_fv_name(fv, config) == "driver_stats_v3"

    def test_projection_version_tag_zero_no_suffix(self):
        from feast.infra.online_stores.bigtable import _versioned_fv_name

        fv = _make_feature_view(version_tag=0, version_number=3)
        config = _make_config(versioning=True)
        assert _versioned_fv_name(fv, config) == "driver_stats"


class TestBigtableRowKeyVersioning:
    """Test that BigtableOnlineStore read/write paths use versioned row keys."""

    def test_different_versions_produce_different_row_keys(self):
        store = BigtableOnlineStore()
        config = _make_config(versioning=True)
        ek = _make_entity_key(1)

        key_v1 = store._compute_row_key(ek, "driver_stats_v1", config)
        key_v2 = store._compute_row_key(ek, "driver_stats_v2", config)
        assert key_v1 != key_v2
        assert key_v1.endswith(b"#driver_stats_v1")
        assert key_v2.endswith(b"#driver_stats_v2")

    def test_online_write_batch_uses_versioned_row_key(self):
        store = BigtableOnlineStore()
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=2)
        ek = _make_entity_key(1)
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)

        bt_table = MagicMock()
        client = MagicMock()
        client.instance.return_value.table.return_value = bt_table

        with patch.object(BigtableOnlineStore, "_get_client", return_value=client):
            store.online_write_batch(
                config,
                fv,
                [(ek, {"trips_today": ValueProto(float_val=5.0)}, ts, ts)],
                None,
            )

        expected_entity_id = compute_entity_id(ek, entity_key_serialization_version=3)
        bt_table.direct_row.assert_called_once_with(
            f"{expected_entity_id}#driver_stats_v2".encode()
        )

    def test_online_write_batch_unversioned_when_disabled(self):
        store = BigtableOnlineStore()
        config = _make_config(versioning=False)
        fv = _make_feature_view(version_number=2)
        ek = _make_entity_key(1)
        ts = datetime(2025, 1, 1, tzinfo=timezone.utc)

        bt_table = MagicMock()
        client = MagicMock()
        client.instance.return_value.table.return_value = bt_table

        with patch.object(BigtableOnlineStore, "_get_client", return_value=client):
            store.online_write_batch(
                config,
                fv,
                [(ek, {"trips_today": ValueProto(float_val=5.0)}, ts, ts)],
                None,
            )

        expected_entity_id = compute_entity_id(ek, entity_key_serialization_version=3)
        bt_table.direct_row.assert_called_once_with(
            f"{expected_entity_id}#driver_stats".encode()
        )

    def test_online_read_uses_versioned_row_key(self):
        store = BigtableOnlineStore()
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=2)
        ek = _make_entity_key(1)

        bt_table = MagicMock()
        bt_table.read_rows.return_value = []
        client = MagicMock()
        client.instance.return_value.table.return_value = bt_table

        with patch.object(BigtableOnlineStore, "_get_client", return_value=client):
            results = store.online_read(config, fv, [ek], ["trips_today"])

        assert results == [(None, None)]
        row_set = bt_table.read_rows.call_args.kwargs["row_set"]
        expected_entity_id = compute_entity_id(ek, entity_key_serialization_version=3)
        assert row_set.row_keys == [f"{expected_entity_id}#driver_stats_v2".encode()]

    def test_online_read_uses_projection_version_tag(self):
        store = BigtableOnlineStore()
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=1, version_tag=3)
        ek = _make_entity_key(1)

        bt_table = MagicMock()
        bt_table.read_rows.return_value = []
        client = MagicMock()
        client.instance.return_value.table.return_value = bt_table

        with patch.object(BigtableOnlineStore, "_get_client", return_value=client):
            store.online_read(config, fv, [ek], ["trips_today"])

        row_set = bt_table.read_rows.call_args.kwargs["row_set"]
        expected_entity_id = compute_entity_id(ek, entity_key_serialization_version=3)
        assert row_set.row_keys == [f"{expected_entity_id}#driver_stats_v3".encode()]


class TestBigtableVersionedReadSupport:
    """Test that BigtableOnlineStore passes _check_versioned_read_support."""

    def test_is_versioned_read_supported(self):
        store = BigtableOnlineStore()
        assert store._is_versioned_read_supported() is True

    def test_allowed_with_version_tag(self):
        store = BigtableOnlineStore()
        fv = _make_feature_view()
        fv.projection.version_tag = 2
        store._check_versioned_read_support([(fv, ["trips_today"])])

    def test_allowed_without_version_tag(self):
        store = BigtableOnlineStore()
        fv = _make_feature_view()
        store._check_versioned_read_support([(fv, ["trips_today"])])
