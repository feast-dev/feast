from unittest.mock import Mock, patch

from feast.feature_store import FeatureStore
from feast.infra.mcp_servers.mcp_config import McpFeatureServerConfig


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
        assert config.transformation_service_endpoint == "localhost:6566"

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

        assert config.type == "mcp"
        assert config.enabled is True
        assert config.mcp_enabled is True
        assert config.mcp_server_name == "custom-feast-server"
        assert config.mcp_server_version == "2.0.0"
        assert config.mcp_transport == "sse"
        assert config.transformation_service_endpoint == "custom-host:8080"


@patch("feast.infra.mcp_servers.mcp_server.MCP_AVAILABLE", True)
class TestMCPIntegration:
    """Test MCP integration functionality."""

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
        assert result == mock_mcp_instance

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

        assert result == mock_mcp_instance


@patch("feast.infra.mcp_servers.mcp_server.MCP_AVAILABLE", False)
class TestMCPNotAvailable:
    """Test behavior when MCP is not available."""

    def test_add_mcp_support_mcp_not_available(self):
        """Test add_mcp_support_to_app when MCP is not available."""
        from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

        mock_app = Mock()
        mock_store = Mock()
        mock_config = Mock()

        result = add_mcp_support_to_app(mock_app, mock_store, mock_config)

        assert result is None


class TestFeatureServerIntegration:
    """Test integration with feature server."""

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
