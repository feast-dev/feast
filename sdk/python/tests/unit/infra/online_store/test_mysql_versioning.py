"""Unit tests for MySQL online store feature view versioning."""

from datetime import timedelta

from feast import Entity, FeatureView
from feast.field import Field
from feast.infra.online_stores.mysql_online_store.mysql import (
    MySQLOnlineStore,
    _table_id,
)
from feast.types import Float32, Int64
from feast.value_type import ValueType


def _make_feature_view(name="driver_stats", version="latest"):
    entity = Entity(
        name="driver_id",
        join_keys=["driver_id"],
        value_type=ValueType.INT64,
    )
    return FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="trips_today", dtype=Int64),
            Field(name="avg_rating", dtype=Float32),
        ],
        version=version,
    )


class TestMySQLTableId:
    """Test _table_id generates correct versioned table names."""

    def test_default_no_versioning(self):
        fv = _make_feature_view()
        assert _table_id("proj", fv) == "proj_driver_stats"

    def test_versioning_explicitly_disabled(self):
        fv = _make_feature_view()
        assert _table_id("proj", fv, enable_versioning=False) == "proj_driver_stats"

    def test_versioning_enabled_no_version_set(self):
        fv = _make_feature_view()
        assert _table_id("proj", fv, enable_versioning=True) == "proj_driver_stats"

    def test_versioning_enabled_with_current_version_number(self):
        fv = _make_feature_view()
        fv.current_version_number = 2
        assert _table_id("proj", fv, enable_versioning=True) == "proj_driver_stats_v2"

    def test_version_zero_no_suffix(self):
        fv = _make_feature_view()
        fv.current_version_number = 0
        assert _table_id("proj", fv, enable_versioning=True) == "proj_driver_stats"

    def test_projection_version_tag_takes_priority(self):
        fv = _make_feature_view()
        fv.current_version_number = 2
        fv.projection.version_tag = 4
        assert _table_id("proj", fv, enable_versioning=True) == "proj_driver_stats_v4"

    def test_projection_version_tag_zero_no_suffix(self):
        fv = _make_feature_view()
        fv.projection.version_tag = 0
        fv.current_version_number = 1
        assert _table_id("proj", fv, enable_versioning=True) == "proj_driver_stats"

    def test_different_project_names(self):
        fv = _make_feature_view()
        fv.current_version_number = 1
        assert _table_id("prod", fv, enable_versioning=True) == "prod_driver_stats_v1"
        assert (
            _table_id("staging", fv, enable_versioning=True)
            == "staging_driver_stats_v1"
        )

    def test_different_feature_view_names(self):
        fv = _make_feature_view(name="user_stats")
        fv.current_version_number = 2
        assert _table_id("proj", fv, enable_versioning=True) == "proj_user_stats_v2"


class TestMySQLVersionedReadSupport:
    """Test that MySQLOnlineStore passes _check_versioned_read_support."""

    def test_allowed_with_version_tag(self):
        store = MySQLOnlineStore()
        fv = _make_feature_view()
        fv.projection.version_tag = 2
        store._check_versioned_read_support([(fv, ["trips_today"])])

    def test_allowed_without_version_tag(self):
        store = MySQLOnlineStore()
        fv = _make_feature_view()
        store._check_versioned_read_support([(fv, ["trips_today"])])
