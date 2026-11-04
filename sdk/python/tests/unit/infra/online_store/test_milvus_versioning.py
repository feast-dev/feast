"""Unit tests for Milvus online store feature view versioning."""

from datetime import timedelta
from unittest.mock import MagicMock

from feast import Entity, FeatureView
from feast.field import Field
from feast.types import Float32
from feast.value_type import ValueType


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
    config.entity_key_serialization_version = 2
    config.registry.enable_online_feature_view_versioning = versioning
    return config


class TestTableId:
    """Test _table_id with versioning enabled/disabled."""

    def test_no_versioning(self):
        from feast.infra.online_stores.milvus_online_store.milvus import _table_id

        fv = _make_feature_view()
        config = _make_config(versioning=False)
        assert _table_id(config.project, fv) == "test_project_driver_stats"

    def test_versioning_enabled_with_version(self):
        from feast.infra.online_stores.milvus_online_store.milvus import _table_id

        fv = _make_feature_view(version_number=2)
        config = _make_config(versioning=True)
        assert (
            _table_id(config.project, fv, enable_versioning=True)
            == "test_project_driver_stats_v2"
        )

    def test_projection_version_tag_takes_priority(self):
        from feast.infra.online_stores.milvus_online_store.milvus import _table_id

        fv = _make_feature_view(version_number=1, version_tag=3)
        config = _make_config(versioning=True)
        assert (
            _table_id(config.project, fv, enable_versioning=True)
            == "test_project_driver_stats_v3"
        )

    def test_version_zero_no_suffix(self):
        from feast.infra.online_stores.milvus_online_store.milvus import _table_id

        fv = _make_feature_view(version_number=0)
        config = _make_config(versioning=True)
        assert (
            _table_id(config.project, fv, enable_versioning=True)
            == "test_project_driver_stats"
        )

    def test_versioning_enabled_no_version_set(self):
        from feast.infra.online_stores.milvus_online_store.milvus import _table_id

        fv = _make_feature_view()
        config = _make_config(versioning=True)
        assert (
            _table_id(config.project, fv, enable_versioning=True)
            == "test_project_driver_stats"
        )

    def test_versioning_disabled_ignores_version(self):
        from feast.infra.online_stores.milvus_online_store.milvus import _table_id

        fv = _make_feature_view(version_number=5)
        config = _make_config(versioning=False)
        assert _table_id(config.project, fv) == "test_project_driver_stats"


class TestMilvusVersionedReadSupport:
    """Test that MilvusOnlineStore passes _check_versioned_read_support."""

    def test_allowed_with_version_tag(self):
        from feast.infra.online_stores.milvus_online_store.milvus import (
            MilvusOnlineStore,
        )

        store = MilvusOnlineStore()
        fv = _make_feature_view()
        fv.projection.version_tag = 2
        store._check_versioned_read_support([(fv, ["trips_today"])])

    def test_allowed_without_version_tag(self):
        from feast.infra.online_stores.milvus_online_store.milvus import (
            MilvusOnlineStore,
        )

        store = MilvusOnlineStore()
        fv = _make_feature_view()
        store._check_versioned_read_support([(fv, ["trips_today"])])


class TestTeardownDropsAllVersions:
    """Teardown should drop the base collection AND all versioned collections."""

    def _build_store_with_collections(self, existing_collections):
        from feast.infra.online_stores.milvus_online_store.milvus import (
            MilvusOnlineStore,
        )

        store = MilvusOnlineStore()
        store.client = MagicMock()
        store.client.list_collections.return_value = existing_collections
        store._connect = MagicMock(return_value=store.client)
        store._collections = {name: MagicMock() for name in existing_collections}
        return store

    def test_teardown_drops_base_and_all_versioned_collections(self):
        fv = _make_feature_view()
        config = _make_config(versioning=True)
        existing = [
            "test_project_driver_stats",
            "test_project_driver_stats_v1",
            "test_project_driver_stats_v2",
            "test_project_other_view",  # unrelated, must not be dropped
        ]
        store = self._build_store_with_collections(existing)

        store.teardown(config, [fv], [])

        dropped = {call.args[0] for call in store.client.drop_collection.call_args_list}
        assert dropped == {
            "test_project_driver_stats",
            "test_project_driver_stats_v1",
            "test_project_driver_stats_v2",
        }
        assert "test_project_other_view" not in dropped

    def test_update_drops_all_versions_for_deleted_table(self):
        fv = _make_feature_view()
        config = _make_config(versioning=True)
        existing = [
            "test_project_driver_stats",
            "test_project_driver_stats_v3",
            "test_project_driver_stats_v4",
        ]
        store = self._build_store_with_collections(existing)

        store.update(
            config=config,
            tables_to_delete=[fv],
            tables_to_keep=[],
            entities_to_delete=[],
            entities_to_keep=[],
            partial=False,
        )

        dropped = {call.args[0] for call in store.client.drop_collection.call_args_list}
        assert dropped == {
            "test_project_driver_stats",
            "test_project_driver_stats_v3",
            "test_project_driver_stats_v4",
        }
