"""Unit tests for Snowflake online store feature view versioning."""

import sys
from datetime import timedelta
from types import ModuleType
from unittest.mock import MagicMock

from feast import Entity, FeatureView
from feast.field import Field
from feast.types import Float32
from feast.value_type import ValueType


def _stub_snowflake_modules():
    """Stub out Snowflake connector and cryptography so the online store can be imported."""

    # Build a proper package hierarchy so submodule imports don't fail.
    def _mod(name):
        m = ModuleType(name)
        sys.modules[name] = m
        return m

    if "cryptography" not in sys.modules:
        crypto = _mod("cryptography")
        hazmat = _mod("cryptography.hazmat")
        backends = _mod("cryptography.hazmat.backends")
        backends.default_backend = MagicMock()
        primitives = _mod("cryptography.hazmat.primitives")
        serialization = _mod("cryptography.hazmat.primitives.serialization")
        serialization.Encoding = MagicMock()
        serialization.PrivateFormat = MagicMock()
        serialization.NoEncryption = MagicMock()
        crypto.hazmat = hazmat
        hazmat.backends = backends
        hazmat.primitives = primitives
        primitives.serialization = serialization

    if "snowflake" not in sys.modules:
        sf = _mod("snowflake")
        connector = _mod("snowflake.connector")
        connector.ProgrammingError = Exception
        connector.SnowflakeConnection = MagicMock()
        cursor_mod = _mod("snowflake.connector.cursor")
        cursor_mod.SnowflakeCursor = MagicMock()
        sf.connector = connector
        connector.cursor = cursor_mod

    if "tenacity" not in sys.modules:
        tenacity = _mod("tenacity")
        tenacity.retry = lambda *a, **kw: lambda f: f
        tenacity.retry_if_exception_type = MagicMock()
        tenacity.stop_after_attempt = MagicMock()
        tenacity.wait_exponential = MagicMock()


_stub_snowflake_modules()


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


class TestSnowflakeTableName:
    """Test _snowflake_table_name with versioning enabled/disabled."""

    def test_no_versioning(self):
        from feast.infra.online_stores.snowflake import _snowflake_table_name

        fv = _make_feature_view()
        assert (
            _snowflake_table_name("test_project", fv, False)
            == "[online-transient] test_project_driver_stats"
        )

    def test_versioning_disabled_ignores_version(self):
        from feast.infra.online_stores.snowflake import _snowflake_table_name

        fv = _make_feature_view(version_number=5)
        assert (
            _snowflake_table_name("test_project", fv, False)
            == "[online-transient] test_project_driver_stats"
        )

    def test_versioning_enabled_no_version_set(self):
        from feast.infra.online_stores.snowflake import _snowflake_table_name

        fv = _make_feature_view()
        assert (
            _snowflake_table_name("test_project", fv, True)
            == "[online-transient] test_project_driver_stats"
        )

    def test_versioning_enabled_with_current_version_number(self):
        from feast.infra.online_stores.snowflake import _snowflake_table_name

        fv = _make_feature_view(version_number=2)
        assert (
            _snowflake_table_name("test_project", fv, True)
            == "[online-transient] test_project_driver_stats_v2"
        )

    def test_version_zero_no_suffix(self):
        from feast.infra.online_stores.snowflake import _snowflake_table_name

        fv = _make_feature_view(version_number=0)
        assert (
            _snowflake_table_name("test_project", fv, True)
            == "[online-transient] test_project_driver_stats"
        )

    def test_projection_version_tag_takes_priority(self):
        from feast.infra.online_stores.snowflake import _snowflake_table_name

        fv = _make_feature_view(version_number=1, version_tag=3)
        assert (
            _snowflake_table_name("test_project", fv, True)
            == "[online-transient] test_project_driver_stats_v3"
        )

    def test_projection_version_tag_zero_no_suffix(self):
        from feast.infra.online_stores.snowflake import _snowflake_table_name

        fv = _make_feature_view(version_tag=0, version_number=3)
        assert (
            _snowflake_table_name("test_project", fv, True)
            == "[online-transient] test_project_driver_stats"
        )

    def test_different_versions_produce_different_table_names(self):
        from feast.infra.online_stores.snowflake import _snowflake_table_name

        fv_v1 = _make_feature_view(version_number=1)
        fv_v2 = _make_feature_view(version_number=2)
        name_v1 = _snowflake_table_name("prod", fv_v1, True)
        name_v2 = _snowflake_table_name("prod", fv_v2, True)
        assert name_v1 != name_v2
        assert name_v1 == "[online-transient] prod_driver_stats_v1"
        assert name_v2 == "[online-transient] prod_driver_stats_v2"


class TestSnowflakeVersionedReadSupport:
    """Test that SnowflakeOnlineStore passes _check_versioned_read_support."""

    def test_allowed_with_version_tag(self):
        from feast.infra.online_stores.snowflake import SnowflakeOnlineStore

        store = SnowflakeOnlineStore()
        fv = _make_feature_view()
        fv.projection.version_tag = 2
        store._check_versioned_read_support([(fv, ["trips_today"])])

    def test_allowed_without_version_tag(self):
        from feast.infra.online_stores.snowflake import SnowflakeOnlineStore

        store = SnowflakeOnlineStore()
        fv = _make_feature_view()
        store._check_versioned_read_support([(fv, ["trips_today"])])
