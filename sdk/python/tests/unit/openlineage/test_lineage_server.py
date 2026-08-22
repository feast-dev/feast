"""Tests for the standalone lineage server (lineage_server.py)."""

from unittest.mock import MagicMock

import pytest

from feast.openlineage.config import OpenLineageConfig, OpenLineageConsumerConfig


class TestCreateLineageApp:
    """Test create_lineage_app factory."""

    def _make_mock_store(self, ol_config=None, registry_path=None):
        store = MagicMock()
        store.config = MagicMock()
        store.config.openlineage = ol_config
        store.config.auth = None
        store.config.auth_config = None

        if registry_path:
            store.config.registry = MagicMock()
            store.config.registry.path = registry_path
        else:
            store.config.registry = MagicMock()
            store.config.registry.path = None

        return store

    def test_raises_when_no_openlineage_config(self):
        from feast.lineage_server import create_lineage_app

        store = self._make_mock_store(ol_config=None)
        with pytest.raises(ValueError, match="OpenLineage configuration is required"):
            create_lineage_app(store)

    def test_raises_when_consumer_disabled(self):
        from feast.lineage_server import create_lineage_app

        store = self._make_mock_store(
            ol_config=OpenLineageConfig(
                enabled=True,
                consumer=OpenLineageConsumerConfig(enabled=False),
            )
        )
        with pytest.raises(ValueError, match="consumer must be enabled"):
            create_lineage_app(store)

    def test_creates_app_with_explicit_connection_string(self):
        from feast.lineage_server import create_lineage_app

        store = self._make_mock_store(
            ol_config=OpenLineageConfig(
                enabled=True,
                consumer=OpenLineageConsumerConfig(
                    enabled=True,
                    connection_string="sqlite://",
                ),
            )
        )
        app = create_lineage_app(store)
        assert app is not None
        assert app.title == "Feast OpenLineage Server"

    def test_creates_app_with_registry_fallback(self):
        from feast.lineage_server import create_lineage_app

        store = self._make_mock_store(
            ol_config=OpenLineageConfig(
                enabled=True,
                consumer=OpenLineageConsumerConfig(
                    enabled=True,
                    connection_string=None,
                ),
            ),
            registry_path="sqlite://",
        )
        app = create_lineage_app(store)
        assert app is not None

    def test_raises_when_no_db_available(self):
        from feast.lineage_server import create_lineage_app

        store = self._make_mock_store(
            ol_config=OpenLineageConfig(
                enabled=True,
                consumer=OpenLineageConsumerConfig(
                    enabled=True,
                    connection_string=None,
                ),
            ),
            registry_path=None,
        )
        with pytest.raises(ValueError, match="SQL database"):
            create_lineage_app(store)

    def test_retention_disabled_when_zero(self):
        from feast.lineage_server import create_lineage_app

        store = self._make_mock_store(
            ol_config=OpenLineageConfig(
                enabled=True,
                consumer=OpenLineageConsumerConfig(
                    enabled=True,
                    connection_string="sqlite://",
                    retention_days=0,
                ),
            )
        )
        app = create_lineage_app(store)
        assert app is not None

    def test_app_has_lineage_endpoints(self):
        from feast.lineage_server import create_lineage_app

        store = self._make_mock_store(
            ol_config=OpenLineageConfig(
                enabled=True,
                consumer=OpenLineageConsumerConfig(
                    enabled=True,
                    connection_string="sqlite://",
                ),
            )
        )
        app = create_lineage_app(store)

        openapi = app.openapi()
        paths = list(openapi.get("paths", {}).keys())
        assert any("/lineage" in p for p in paths), f"No /lineage path in {paths}"


class TestStandaloneServerFlag:
    """Test that standalone_server flag controls embedded consumer."""

    def test_standalone_server_skips_embedded_consumer(self):
        """Verify the standalone_server flag is correctly propagated."""
        config = OpenLineageConsumerConfig(
            enabled=True,
            standalone_server=True,
        )
        assert config.standalone_server is True

        d = config.to_dict()
        assert d["standalone_server"] is True

        restored = OpenLineageConsumerConfig.from_dict(d)
        assert restored.standalone_server is True

    def test_standalone_server_defaults_false(self):
        config = OpenLineageConsumerConfig(enabled=True)
        assert config.standalone_server is False


class TestBuildRbacCallback:
    """Test _build_rbac_callback."""

    def test_returns_none_without_authz(self):
        from feast.lineage_server import _build_rbac_callback

        store = MagicMock()
        store.config = MagicMock()
        store.config.auth = None
        store.config.auth_config = None

        result = _build_rbac_callback(store)
        assert result is None
