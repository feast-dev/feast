"""Unit tests for Redis online store feature view versioning."""

from datetime import timedelta
from unittest.mock import MagicMock

from feast import Entity, FeatureView
from feast.field import Field
from feast.infra.online_stores.helpers import _mmh3
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


class TestVersionedFvName:
    """Test _versioned_fv_name produces correct versioned names."""

    def test_no_versioning(self):
        from feast.infra.online_stores.redis import _versioned_fv_name

        fv = _make_feature_view()
        config = _make_config(versioning=False)
        assert _versioned_fv_name(fv, config) == "driver_stats"

    def test_versioning_disabled_ignores_version(self):
        from feast.infra.online_stores.redis import _versioned_fv_name

        fv = _make_feature_view(version_number=3)
        config = _make_config(versioning=False)
        assert _versioned_fv_name(fv, config) == "driver_stats"

    def test_versioning_enabled_no_version_set(self):
        from feast.infra.online_stores.redis import _versioned_fv_name

        fv = _make_feature_view()
        config = _make_config(versioning=True)
        assert _versioned_fv_name(fv, config) == "driver_stats"

    def test_versioning_enabled_with_current_version_number(self):
        from feast.infra.online_stores.redis import _versioned_fv_name

        fv = _make_feature_view(version_number=2)
        config = _make_config(versioning=True)
        assert _versioned_fv_name(fv, config) == "driver_stats_v2"

    def test_version_zero_no_suffix(self):
        from feast.infra.online_stores.redis import _versioned_fv_name

        fv = _make_feature_view(version_number=0)
        config = _make_config(versioning=True)
        assert _versioned_fv_name(fv, config) == "driver_stats"

    def test_projection_version_tag_takes_priority(self):
        from feast.infra.online_stores.redis import _versioned_fv_name

        fv = _make_feature_view(version_number=1, version_tag=3)
        config = _make_config(versioning=True)
        assert _versioned_fv_name(fv, config) == "driver_stats_v3"

    def test_projection_version_tag_zero_no_suffix(self):
        from feast.infra.online_stores.redis import _versioned_fv_name

        fv = _make_feature_view(version_tag=0, version_number=3)
        config = _make_config(versioning=True)
        assert _versioned_fv_name(fv, config) == "driver_stats"


class TestHsetKeysVersioning:
    """Test that _generate_hset_keys_for_features produces different keys for different versions."""

    def test_different_versions_produce_different_hset_keys(self):
        from feast.infra.online_stores.redis import RedisOnlineStore

        store = RedisOnlineStore()
        fv = _make_feature_view()

        _, hset_keys_v1 = store._generate_hset_keys_for_features(
            fv, ["trips_today"], fv_name_override="driver_stats_v1"
        )
        _, hset_keys_v2 = store._generate_hset_keys_for_features(
            fv, ["trips_today"], fv_name_override="driver_stats_v2"
        )

        # The mmh3 hash keys should differ
        assert hset_keys_v1[0] != hset_keys_v2[0]
        # The timestamp keys should also differ
        assert hset_keys_v1[1] != hset_keys_v2[1]

    def test_no_override_uses_fv_name(self):
        from feast.infra.online_stores.redis import RedisOnlineStore

        store = RedisOnlineStore()
        fv = _make_feature_view()

        _, hset_keys = store._generate_hset_keys_for_features(fv, ["trips_today"])

        expected_feature_key = _mmh3("driver_stats:trips_today")
        expected_ts_key = "_ts:driver_stats"
        assert hset_keys[0] == expected_feature_key
        assert hset_keys[1] == expected_ts_key


class TestRedisVersionedReadSupport:
    """Test that RedisOnlineStore passes _check_versioned_read_support."""

    def test_allowed_with_version_tag(self):
        from feast.infra.online_stores.redis import RedisOnlineStore

        store = RedisOnlineStore()
        fv = _make_feature_view()
        fv.projection.version_tag = 2
        # Should not raise
        store._check_versioned_read_support([(fv, ["trips_today"])])

    def test_allowed_without_version_tag(self):
        from feast.infra.online_stores.redis import RedisOnlineStore

        store = RedisOnlineStore()
        fv = _make_feature_view()
        store._check_versioned_read_support([(fv, ["trips_today"])])
