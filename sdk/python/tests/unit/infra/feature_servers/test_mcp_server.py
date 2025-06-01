import unittest
from unittest.mock import Mock, patch

from feast.feature_store import FeatureStore
from feast.infra.mcp_servers.mcp_config import McpFeatureServerConfig


class TestMcpFeatureServerConfig(unittest.TestCase):
    """Test MCP feature server configuration."""

    def test_default_config(self):
        """Test default MCP configuration values."""
        config = McpFeatureServerConfig()

        self.assertEqual(config.type, "mcp")
        self.assertFalse(config.enabled)
        self.assertFalse(config.mcp_enabled)
        self.assertEqual(config.mcp_server_name, "feast-mcp-server")
        self.assertEqual(config.mcp_server_version, "1.0.0")
        self.assertIsNone(config.mcp_transport)
        self.assertEqual(config.transformation_service_endpoint, "localhost:6566")

    def test_custom_config(self):
        """Test custom MCP configuration values."""
        config = McpFeatureServerConfig(
            enabled=True,
            mcp_enabled=True,
            mcp_server_name="custom-feast-server",
            mcp_server_version="2.0.0",
            mcp_transport="sse",
            transformation_service_endpoint="custom-host:8080",
        )

        self.assertEqual(config.type, "mcp")
        self.assertTrue(config.enabled)
        self.assertTrue(config.mcp_enabled)
        self.assertEqual(config.mcp_server_name, "custom-feast-server")
        self.assertEqual(config.mcp_server_version, "2.0.0")
        self.assertEqual(config.mcp_transport, "sse")
        self.assertEqual(config.transformation_service_endpoint, "custom-host:8080")

    def test_config_validation(self):
        """Test configuration validation."""
        # Test valid transport options
        valid_transports = ["sse", "websocket", None]
        for transport in valid_transports:
            config = McpFeatureServerConfig(mcp_transport=transport)
            self.assertEqual(config.mcp_transport, transport)

    def test_config_inheritance(self):
        """Test that McpFeatureServerConfig properly inherits from BaseFeatureServerConfig."""
        config = McpFeatureServerConfig()
        # Verify it has the base configuration attributes
        self.assertTrue(hasattr(config, 'type'))
        self.assertTrue(hasattr(config, 'enabled'))


@patch("feast.infra.mcp_servers.mcp_server.MCP_AVAILABLE", True)
class TestMCPServerUnit(unittest.TestCase):
    """Unit tests for MCP server functionality with mocked dependencies."""

    @patch("feast.infra.mcp_servers.mcp_server.FastApiMCP")
    def test_add_mcp_support_success(self, mock_fast_api_mcp):
        """Test successful MCP support addition."""
        from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

        mock_app = Mock()
        mock_store = Mock(spec=FeatureStore)
        mock_config = Mock()
        mock_config.mcp_server_name = "test-server"
        mock_config.mcp_server_version = "1.0.0"

        # Mock the FastApiMCP instance
        mock_mcp_instance = Mock()
        mock_fast_api_mcp.return_value = mock_mcp_instance

        result = add_mcp_support_to_app(mock_app, mock_store, mock_config)

        # Verify FastApiMCP was called correctly
        mock_fast_api_mcp.assert_called_once_with(
            mock_app,
            name="test-server",
            description="Feast Feature Store MCP Server - Access feature store data and operations through MCP",
        )

        # Verify mount was called
        mock_mcp_instance.mount.assert_called_once()

        # Verify the result
        self.assertEqual(result, mock_mcp_instance)

    @patch("feast.infra.mcp_servers.mcp_server.FastApiMCP")
    def test_add_mcp_support_with_defaults(self, mock_fast_api_mcp):
        """Test MCP support addition with default configuration values."""
        from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

        mock_app = Mock()
        mock_store = Mock(spec=FeatureStore)
        mock_config = Mock()
        # Don't set mcp_server_name to test default
        del mock_config.mcp_server_name

        mock_mcp_instance = Mock()
        mock_fast_api_mcp.return_value = mock_mcp_instance

        result = add_mcp_support_to_app(mock_app, mock_store, mock_config)

        # Verify FastApiMCP was called with default name
        mock_fast_api_mcp.assert_called_once_with(
            mock_app,
            name="feast-feature-store",
            description="Feast Feature Store MCP Server - Access feature store data and operations through MCP",
        )

        self.assertEqual(result, mock_mcp_instance)

    @patch("feast.infra.mcp_servers.mcp_server.FastApiMCP")
    @patch("feast.infra.mcp_servers.mcp_server.logger")
    def test_add_mcp_support_with_exception(self, mock_logger, mock_fast_api_mcp):
        """Test MCP support addition when FastApiMCP raises an exception."""
        from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

        mock_app = Mock()
        mock_store = Mock(spec=FeatureStore)
        mock_config = Mock()
        mock_config.mcp_server_name = "test-server"

        # Mock FastApiMCP to raise an exception
        mock_fast_api_mcp.side_effect = Exception("MCP initialization failed")

        result = add_mcp_support_to_app(mock_app, mock_store, mock_config)

        # Verify the result is None when exception occurs
        self.assertIsNone(result)
        
        # Verify error was logged
        mock_logger.error.assert_called_once_with("Failed to initialize MCP integration: MCP initialization failed")

    @patch("feast.infra.mcp_servers.mcp_server.FastApiMCP")
    def test_add_mcp_support_mount_exception(self, mock_fast_api_mcp):
        """Test MCP support addition when mount() raises an exception."""
        from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

        mock_app = Mock()
        mock_store = Mock(spec=FeatureStore)
        mock_config = Mock()
        mock_config.mcp_server_name = "test-server"

        mock_mcp_instance = Mock()
        mock_mcp_instance.mount.side_effect = Exception("Mount failed")
        mock_fast_api_mcp.return_value = mock_mcp_instance

        result = add_mcp_support_to_app(mock_app, mock_store, mock_config)

        # Verify the result is None when mount fails
        self.assertIsNone(result)


@patch("feast.infra.mcp_servers.mcp_server.MCP_AVAILABLE", False)
class TestMCPNotAvailable(unittest.TestCase):
    """Test behavior when MCP is not available."""

    @patch("feast.infra.mcp_servers.mcp_server.logger")
    def test_add_mcp_support_mcp_not_available(self, mock_logger):
        """Test add_mcp_support_to_app when MCP is not available."""
        from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

        mock_app = Mock()
        mock_store = Mock()
        mock_config = Mock()

        result = add_mcp_support_to_app(mock_app, mock_store, mock_config)

        self.assertIsNone(result)
        mock_logger.warning.assert_called_once_with("MCP support requested but fastapi_mcp is not available")


class TestFeatureServerMCPHooks(unittest.TestCase):
    """Test MCP hooks in feature server."""

    @patch("feast.feature_server.logger")
    def test_add_mcp_support_if_enabled_exception(self, mock_logger):
        """Test _add_mcp_support_if_enabled when an exception occurs."""
        from feast.feature_server import _add_mcp_support_if_enabled

        mock_app = Mock()
        mock_store = Mock()
        mock_store.config.feature_server = Mock()
        mock_store.config.feature_server.type = "mcp"
        mock_store.config.feature_server.mcp_enabled = True

        # Mock the import to raise an exception
        with patch(
            "feast.infra.mcp_servers.mcp_server.add_mcp_support_to_app",
            side_effect=Exception("Test error"),
        ):
            _add_mcp_support_if_enabled(mock_app, mock_store)

            mock_logger.error.assert_called_with(
                "Error checking/adding MCP support: Test error"
            )
