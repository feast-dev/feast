"""Tests for the standalone lineage server (lineage_server.py)."""

from unittest.mock import MagicMock, patch

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


class TestStartLineageServer:
    """Test start_lineage_server."""

    def _make_mock_store(self):
        store = MagicMock()
        store.config = MagicMock()
        store.config.openlineage = None
        store.config.auth = None
        store.config.auth_config = None
        return store

    @patch("uvicorn.Server")
    @patch("uvicorn.Config")
    @patch("uvicorn.run")
    @patch("feast.lineage_server.create_lineage_app")
    def test_dual_stack_binds_prebuilt_socket(
        self, mock_create_app, mock_run, mock_config_cls, mock_server_cls
    ):
        """host="::" must go through a pre-bound dual-stack socket, not
        uvicorn.run(host="::"), which binds IPv6-only and drops IPv4 clients."""
        from feast.lineage_server import start_lineage_server

        mock_app = MagicMock()
        mock_create_app.return_value = mock_app
        mock_sock = MagicMock()

        with patch(
            "feast.utils._make_dual_stack_socket", return_value=mock_sock
        ) as mock_make_sock:
            start_lineage_server(self._make_mock_store(), host="::", port=6580)

        mock_make_sock.assert_called_once_with(6580)
        mock_config_cls.assert_called_once_with(mock_app)
        mock_server_cls.assert_called_once_with(mock_config_cls.return_value)
        mock_server_cls.return_value.run.assert_called_once_with(sockets=[mock_sock])
        mock_run.assert_not_called()

    @patch("feast.utils._make_dual_stack_socket")
    @patch("uvicorn.Server")
    @patch("uvicorn.Config")
    @patch("uvicorn.run")
    @patch("feast.lineage_server.create_lineage_app")
    def test_plain_host_uses_uvicorn_run(
        self,
        mock_create_app,
        mock_run,
        mock_config_cls,
        mock_server_cls,
        mock_make_sock,
    ):
        """A non dual-stack host keeps today's plain uvicorn.run behavior. Also
        mocks Config/Server/_make_dual_stack_socket so a broken host
        comparison fails fast on an assertion instead of hanging in a real
        uvicorn.Server.run()."""
        from feast.lineage_server import start_lineage_server

        mock_app = MagicMock()
        mock_create_app.return_value = mock_app

        start_lineage_server(self._make_mock_store(), host="0.0.0.0", port=6580)

        mock_run.assert_called_once_with(mock_app, host="0.0.0.0", port=6580)
        mock_server_cls.assert_not_called()
        mock_make_sock.assert_not_called()

    @patch("feast.utils._make_dual_stack_socket")
    @patch("uvicorn.Server")
    @patch("uvicorn.Config")
    @patch("uvicorn.run")
    @patch("feast.lineage_server.create_lineage_app")
    def test_non_wildcard_host_uses_uvicorn_run(
        self,
        mock_create_app,
        mock_run,
        mock_config_cls,
        mock_server_cls,
        mock_make_sock,
    ):
        """A host that sorts after "::" (e.g. starting with a letter) must
        still take the plain uvicorn.run path -- regression test for a
        comparison mutant (host == "::" weakened to <=/>=) that a
        "0.0.0.0"-only test can't catch, since "0.0.0.0" sorts before "::"
        either way."""
        from feast.lineage_server import start_lineage_server

        mock_app = MagicMock()
        mock_create_app.return_value = mock_app

        start_lineage_server(self._make_mock_store(), host="example.com", port=6580)

        mock_run.assert_called_once_with(mock_app, host="example.com", port=6580)
        mock_server_cls.assert_not_called()
        mock_make_sock.assert_not_called()
