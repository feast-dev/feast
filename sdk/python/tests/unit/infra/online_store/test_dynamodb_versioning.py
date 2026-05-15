"""Unit tests for DynamoDB online store feature view versioning."""

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


def _make_online_config(template="{project}.{table_name}"):
    online_config = MagicMock()
    online_config.table_name_template = template
    return online_config


class TestGetTableName:
    """Test _get_table_name with versioning enabled/disabled."""

    def test_no_versioning(self):
        from feast.infra.online_stores.dynamodb import _get_table_name

        fv = _make_feature_view()
        config = _make_config(versioning=False)
        online_config = _make_online_config()
        assert _get_table_name(online_config, config, fv) == "test_project.driver_stats"

    def test_versioning_enabled_with_version(self):
        from feast.infra.online_stores.dynamodb import _get_table_name

        fv = _make_feature_view(version_number=2)
        config = _make_config(versioning=True)
        online_config = _make_online_config()
        assert (
            _get_table_name(online_config, config, fv) == "test_project.driver_stats_v2"
        )

    def test_projection_version_tag_takes_priority(self):
        from feast.infra.online_stores.dynamodb import _get_table_name

        fv = _make_feature_view(version_number=1, version_tag=3)
        config = _make_config(versioning=True)
        online_config = _make_online_config()
        assert (
            _get_table_name(online_config, config, fv) == "test_project.driver_stats_v3"
        )

    def test_version_zero_no_suffix(self):
        from feast.infra.online_stores.dynamodb import _get_table_name

        fv = _make_feature_view(version_number=0)
        config = _make_config(versioning=True)
        online_config = _make_online_config()
        assert _get_table_name(online_config, config, fv) == "test_project.driver_stats"

    def test_versioning_enabled_no_version_set(self):
        from feast.infra.online_stores.dynamodb import _get_table_name

        fv = _make_feature_view()
        config = _make_config(versioning=True)
        online_config = _make_online_config()
        assert _get_table_name(online_config, config, fv) == "test_project.driver_stats"

    def test_custom_template_with_versioning(self):
        from feast.infra.online_stores.dynamodb import _get_table_name

        fv = _make_feature_view(version_number=2)
        config = _make_config(project="prod", versioning=True)
        online_config = _make_online_config(template="feast-{project}-{table_name}")
        assert (
            _get_table_name(online_config, config, fv) == "feast-prod-driver_stats_v2"
        )

    def test_versioning_disabled_ignores_version(self):
        from feast.infra.online_stores.dynamodb import _get_table_name

        fv = _make_feature_view(version_number=5)
        config = _make_config(versioning=False)
        online_config = _make_online_config()
        assert _get_table_name(online_config, config, fv) == "test_project.driver_stats"


class TestDynamoDBVersionedReadSupport:
    """Test that DynamoDBOnlineStore passes _check_versioned_read_support."""

    def test_allowed_with_version_tag(self):
        from feast.infra.online_stores.dynamodb import DynamoDBOnlineStore

        store = DynamoDBOnlineStore()
        fv = _make_feature_view()
        fv.projection.version_tag = 2
        # Should not raise
        store._check_versioned_read_support([(fv, ["trips_today"])])

    def test_allowed_without_version_tag(self):
        from feast.infra.online_stores.dynamodb import DynamoDBOnlineStore

        store = DynamoDBOnlineStore()
        fv = _make_feature_view()
        store._check_versioned_read_support([(fv, ["trips_today"])])
