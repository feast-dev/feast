from unittest.mock import Mock, patch

from feast.feature_store import FeatureStore
from feast.infra.feature_servers.mcp_config import McpFeatureServerConfig


class TestMcpFeatureServerConfig:
    """Test MCP feature server configuration."""

    def test_default_config(self):
        """Test default MCP configuration values."""
        config = McpFeatureServerConfig()

        assert config.type == "mcp"
        assert config.enabled is False
        assert config.mcp_enabled is False
        assert config.mcp_server_name == "feast-mcp-server"
        assert config.mcp_server_version == "1.0.0"
        assert config.mcp_transport is None
        assert config.transformation_service_endpoint == "localhost:6569"

    def test_custom_config(self):
        """Test custom MCP configuration values."""
        config = McpFeatureServerConfig(
            enabled=True,
            mcp_enabled=True,
            mcp_server_name="custom-feast-server",
            mcp_server_version="2.0.0",
            mcp_transport="http"
        )

        assert config.type == "mcp"
        assert config.enabled is True
        assert config.mcp_enabled is True
        assert config.mcp_server_name == "custom-feast-server"
        assert config.mcp_server_version == "2.0.0"
        assert config.mcp_transport == "http"


class TestMcpServer:
    """Test MCP server functionality."""

    @patch('feast.infra.feature_servers.mcp_server.MCP_AVAILABLE', False)
    def test_mcp_not_available(self):
        """Test behavior when MCP dependencies are not available."""
        from feast.infra.feature_servers.mcp_server import add_mcp_support_to_app

        mock_app = Mock()
        mock_store = Mock()
        mock_config = Mock()
        mock_config.mcp_enabled = True

        result = add_mcp_support_to_app(mock_app, mock_store, mock_config)
        assert result is None

    def test_mcp_disabled_in_config(self):
        """Test behavior when MCP is disabled in configuration."""
        from feast.infra.feature_servers.mcp_server import add_mcp_support_to_app

        mock_app = Mock()
        mock_store = Mock()
        mock_config = Mock()
        mock_config.mcp_enabled = False

        result = add_mcp_support_to_app(mock_app, mock_store, mock_config)
        assert result is None

    def test_mcp_config_none(self):
        """Test behavior when config is None."""
        from feast.infra.feature_servers.mcp_server import add_mcp_support_to_app

        mock_app = Mock()
        mock_store = Mock()

        result = add_mcp_support_to_app(mock_app, mock_store, None)
        assert result is None

    @patch('feast.infra.feature_servers.mcp_server.MCP_AVAILABLE', True)
    @patch('feast.infra.feature_servers.mcp_server.FeastMCPServer')
    def test_mcp_server_creation(self, mock_feast_mcp_server):
        """Test MCP server creation when enabled."""
        from feast.infra.feature_servers.mcp_server import add_mcp_support_to_app

        mock_app = Mock()
        mock_store = Mock()
        mock_config = Mock()
        mock_config.mcp_enabled = True
        mock_config.mcp_server_name = "test-server"
        mock_config.mcp_server_version = "1.0.0"

        # Mock the MCP server and integration
        mock_mcp_server_instance = Mock()
        mock_mcp_integration = Mock()
        mock_mcp_server_instance.create_mcp_integration.return_value = mock_mcp_integration
        mock_feast_mcp_server.return_value = mock_mcp_server_instance

        result = add_mcp_support_to_app(mock_app, mock_store, mock_config)

        mock_feast_mcp_server.assert_called_once_with(mock_store, "test-server", "1.0.0")
        mock_mcp_server_instance.create_mcp_integration.assert_called_once_with(mock_app)
        assert result == mock_mcp_server_instance


class TestFeastMCPServer:
    """Test FeastMCPServer class functionality."""

    @patch('feast.infra.feature_servers.mcp_server.MCP_AVAILABLE', True)
    def test_mcp_server_initialization(self):
        """Test MCP server initialization."""
        with patch('feast.infra.feature_servers.mcp_server.FastMCPIntegration'):
            from feast.infra.feature_servers.mcp_server import FeastMCPServer

            mock_store = Mock(spec=FeatureStore)
            server = FeastMCPServer(mock_store, "test-server", "1.0.0")

            assert server.store == mock_store
            assert server.server_name == "test-server"
            assert server.version == "1.0.0"
            assert server._mcp_integration is None

    @patch('feast.infra.feature_servers.mcp_server.MCP_AVAILABLE', True)
    def test_create_mcp_integration_success(self):
        """Test successful MCP integration creation."""
        with patch('feast.infra.feature_servers.mcp_server.FastMCPIntegration') as mock_fast_mcp:
            from feast.infra.feature_servers.mcp_server import FeastMCPServer

            mock_store = Mock(spec=FeatureStore)
            mock_app = Mock()

            # Mock FastMCPIntegration
            mock_integration = Mock()
            mock_fast_mcp.return_value = mock_integration

            server = FeastMCPServer(mock_store, "test-server", "1.0.0")

            # Mock the registration methods
            server._register_tools = Mock()
            server._register_resources = Mock()

            result = server.create_mcp_integration(mock_app)

            mock_fast_mcp.assert_called_once_with(
                server_name="test-server",
                server_version="1.0.0"
            )
            server._register_tools.assert_called_once_with(mock_integration)
            server._register_resources.assert_called_once_with(mock_integration)
            mock_integration.attach_to_app.assert_called_once_with(mock_app)

            assert result == mock_integration
            assert server._mcp_integration == mock_integration

    @patch('feast.infra.feature_servers.mcp_server.MCP_AVAILABLE', True)
    def test_create_mcp_integration_failure(self):
        """Test MCP integration creation failure."""
        with patch('feast.infra.feature_servers.mcp_server.FastMCPIntegration') as mock_fast_mcp:
            from feast.infra.feature_servers.mcp_server import FeastMCPServer

            mock_store = Mock(spec=FeatureStore)
            mock_app = Mock()

            # Mock FastMCPIntegration to raise an exception
            mock_fast_mcp.side_effect = Exception("Integration failed")

            server = FeastMCPServer(mock_store, "test-server", "1.0.0")

            result = server.create_mcp_integration(mock_app)

            assert result is None
            assert server._mcp_integration is None


class TestFeatureServerIntegration:
    """Test integration with the main feature server."""

    @patch('feast.feature_server.logger')
    def test_add_mcp_support_if_enabled_disabled(self, mock_logger):
        """Test _add_mcp_support_if_enabled when MCP is disabled."""
        from feast.feature_server import _add_mcp_support_if_enabled

        mock_app = Mock()
        mock_store = Mock()
        mock_store.config.feature_server = None

        _add_mcp_support_if_enabled(mock_app, mock_store)

        mock_logger.debug.assert_called_with("MCP support is not enabled in feature server configuration")

    @patch('feast.feature_server.logger')
    @patch('feast.infra.feature_servers.mcp_server.add_mcp_support_to_app')
    def test_add_mcp_support_if_enabled_success(self, mock_add_mcp, mock_logger):
        """Test _add_mcp_support_if_enabled when MCP is enabled."""
        from feast.feature_server import _add_mcp_support_if_enabled

        mock_app = Mock()
        mock_store = Mock()
        mock_store.config.feature_server = Mock()
        mock_store.config.feature_server.type = "mcp"
        mock_store.config.feature_server.mcp_enabled = True

        mock_mcp_server = Mock()
        mock_add_mcp.return_value = mock_mcp_server

        _add_mcp_support_if_enabled(mock_app, mock_store)

        mock_add_mcp.assert_called_once_with(mock_app, mock_store, mock_store.config.feature_server)
        mock_logger.info.assert_called_with("MCP support has been enabled for the Feast feature server")

    @patch('feast.feature_server.logger')
    @patch('feast.infra.feature_servers.mcp_server.add_mcp_support_to_app')
    def test_add_mcp_support_if_enabled_failure(self, mock_add_mcp, mock_logger):
        """Test _add_mcp_support_if_enabled when MCP setup fails."""
        from feast.feature_server import _add_mcp_support_if_enabled

        mock_app = Mock()
        mock_store = Mock()
        mock_store.config.feature_server = Mock()
        mock_store.config.feature_server.type = "mcp"
        mock_store.config.feature_server.mcp_enabled = True

        mock_add_mcp.return_value = None

        _add_mcp_support_if_enabled(mock_app, mock_store)

        mock_logger.warning.assert_called_with("MCP support was requested but could not be enabled")

    @patch('feast.feature_server.logger')
    def test_add_mcp_support_if_enabled_exception(self, mock_logger):
        """Test _add_mcp_support_if_enabled when an exception occurs."""
        from feast.feature_server import _add_mcp_support_if_enabled

        mock_app = Mock()
        mock_store = Mock()
        mock_store.config.feature_server = Mock()
        mock_store.config.feature_server.type = "mcp"
        mock_store.config.feature_server.mcp_enabled = True

        # Mock the import to raise an exception
        with patch('feast.infra.feature_servers.mcp_server.add_mcp_support_to_app', side_effect=Exception("Test error")):
            _add_mcp_support_if_enabled(mock_app, mock_store)

        mock_logger.error.assert_called_with("Error checking/adding MCP support: Test error")
