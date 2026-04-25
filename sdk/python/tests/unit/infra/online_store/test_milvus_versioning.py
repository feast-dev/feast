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
        assert _table_id(config.project, fv, enable_versioning=True) == "test_project_driver_stats_v2"

    def test_projection_version_tag_takes_priority(self):
        from feast.infra.online_stores.milvus_online_store.milvus import _table_id

        fv = _make_feature_view(version_number=1, version_tag=3)
        config = _make_config(versioning=True)
        assert _table_id(config.project, fv, enable_versioning=True) == "test_project_driver_stats_v3"

    def test_version_zero_no_suffix(self):
        from feast.infra.online_stores.milvus_online_store.milvus import _table_id

        fv = _make_feature_view(version_number=0)
        config = _make_config(versioning=True)
        assert _table_id(config.project, fv, enable_versioning=True) == "test_project_driver_stats"

    def test_versioning_enabled_no_version_set(self):
        from feast.infra.online_stores.milvus_online_store.milvus import _table_id

        fv = _make_feature_view()
        config = _make_config(versioning=True)
        assert _table_id(config.project, fv, enable_versioning=True) == "test_project_driver_stats"

    def test_versioning_disabled_ignores_version(self):
        from feast.infra.online_stores.milvus_online_store.milvus import _table_id

        fv = _make_feature_view(version_number=5)
        config = _make_config(versioning=False)
        assert _table_id(config.project, fv) == "test_project_driver_stats"


class TestMilvusVersionedReadSupport:
    """Test that MilvusOnlineStore passes _check_versioned_read_support."""

    def test_allowed_with_version_tag(self):
        from feast.infra.online_stores.milvus_online_store.milvus import MilvusOnlineStore

        store = MilvusOnlineStore()
        fv = _make_feature_view()
        fv.projection.version_tag = 2
        store._check_versioned_read_support([(fv, ["trips_today"])])

    def test_allowed_without_version_tag(self):
        from feast.infra.online_stores.milvus_online_store.milvus import MilvusOnlineStore

        store = MilvusOnlineStore()
        fv = _make_feature_view()
        store._check_versioned_read_support([(fv, ["trips_today"])])
