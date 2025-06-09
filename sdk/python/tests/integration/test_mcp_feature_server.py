import unittest
from unittest.mock import MagicMock, Mock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from feast.feature_store import FeatureStore
from feast.infra.mcp_servers.mcp_config import McpFeatureServerConfig


class TestMCPFeatureServerIntegration(unittest.TestCase):
    """Integration tests for MCP feature server functionality."""

    def test_mcp_config_integration(self):
        """Test that MCP configuration integrates properly with the server."""
        config = McpFeatureServerConfig(
            enabled=True,
            mcp_enabled=True,
            mcp_server_name="integration-test-server",
            mcp_server_version="2.1.0",
            mcp_transport="sse",
        )

        # Verify configuration is properly structured for MCP integration
        self.assertTrue(config.enabled)
        self.assertTrue(config.mcp_enabled)
        self.assertEqual(config.mcp_server_name, "integration-test-server")
        self.assertEqual(config.mcp_server_version, "2.1.0")
        self.assertEqual(config.mcp_transport, "sse")

    def test_mcp_server_functionality_with_mock_store(self):
        """Test MCP server functionality with a mock feature store."""
        with patch("feast.infra.mcp_servers.mcp_server.MCP_AVAILABLE", True):
            with patch(
                "feast.infra.mcp_servers.mcp_server.FastApiMCP"
            ) as mock_fast_api_mcp:
                from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

                # Create a more realistic mock feature store
                mock_store = MagicMock(spec=FeatureStore)
                mock_store.list_feature_views.return_value = []
                mock_store.list_data_sources.return_value = []

                mock_app = FastAPI()
                config = McpFeatureServerConfig(
                    mcp_enabled=True,
                    mcp_server_name="test-feast-server",
                    mcp_server_version="1.0.0",
                )

                mock_mcp_instance = Mock()
                mock_fast_api_mcp.return_value = mock_mcp_instance

                result = add_mcp_support_to_app(mock_app, mock_store, config)

                # Verify successful integration
                self.assertIsNotNone(result)
                self.assertEqual(result, mock_mcp_instance)
                mock_fast_api_mcp.assert_called_once()
                mock_mcp_instance.mount.assert_called_once()

    @patch("feast.infra.mcp_servers.mcp_server.MCP_AVAILABLE", True)
    @patch("feast.infra.mcp_servers.mcp_server.FastApiMCP")
    def test_complete_mcp_setup_flow(self, mock_fast_api_mcp):
        """Test the complete MCP setup flow from configuration to mounting."""
        from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

        # Setup test data
        app = FastAPI()
        mock_store = Mock(spec=FeatureStore)
        config = McpFeatureServerConfig(
            enabled=True,
            mcp_enabled=True,
            mcp_server_name="e2e-test-server",
            mcp_server_version="1.0.0",
            transformation_service_endpoint="localhost:6566",
        )

        mock_mcp_instance = Mock()
        mock_fast_api_mcp.return_value = mock_mcp_instance

        # Execute the flow
        result = add_mcp_support_to_app(app, mock_store, config)

        # Verify all steps completed successfully
        self.assertIsNotNone(result)
        mock_fast_api_mcp.assert_called_once_with(
            app,
            name="e2e-test-server",
            description="Feast Feature Store MCP Server - Access feature store data and operations through MCP",
        )
        mock_mcp_instance.mount.assert_called_once()
        self.assertEqual(result, mock_mcp_instance)

    @pytest.mark.skipif(
        condition=True,  # Skip until fastapi_mcp is available
        reason="Requires fastapi_mcp package to be installed",
    )
    def test_real_mcp_integration(self):
        """Test real MCP integration with actual FastAPI app."""
        try:
            from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

            # Create a real FastAPI app
            app = FastAPI()

            # Mock feature store for this test
            mock_store = MagicMock(spec=FeatureStore)
            mock_store.list_feature_views.return_value = []
            mock_store.list_data_sources.return_value = []

            config = McpFeatureServerConfig(
                enabled=True,
                mcp_enabled=True,
                mcp_server_name="real-integration-test",
                mcp_server_version="1.0.0",
            )

            # This would require fastapi_mcp to be installed
            result = add_mcp_support_to_app(app, mock_store, config)

            if result is not None:
                # Test that the app has MCP endpoints
                client = TestClient(app)
                # The exact endpoints would depend on fastapi_mcp implementation
                # Verify the client can be created and make a basic request
                response = client.get("/health", follow_redirects=False)
                # We expect this to either work or return a 404, but not crash
                self.assertIn(response.status_code, [200, 404])
                self.assertIsNotNone(result)
            else:
                # If fastapi_mcp is not available, result should be None
                self.assertIsNone(result)

        except ImportError:
            # Expected if fastapi_mcp is not installed
            self.skipTest("fastapi_mcp not available")

    def test_feature_server_with_mcp_config(self):
        """Test feature server startup with MCP configuration."""
        from feast.feature_server import _add_mcp_support_if_enabled

        app = FastAPI()
        mock_store = Mock()
        mock_store.config.feature_server = McpFeatureServerConfig(
            enabled=True,
            mcp_enabled=True,
            mcp_server_name="feature-server-test",
            mcp_server_version="1.0.0",
        )

        # This should not raise an exception even if MCP is not available
        try:
            _add_mcp_support_if_enabled(app, mock_store)
        except Exception as e:
            # Should handle gracefully
            self.assertIn("MCP", str(e).upper())

    def test_mcp_server_configuration_validation(self):
        """Test comprehensive MCP server configuration validation."""
        # Test various configuration combinations
        test_configs = [
            {
                "enabled": True,
                "mcp_enabled": True,
                "mcp_server_name": "test-server-1",
                "mcp_server_version": "1.0.0",
                "mcp_transport": "sse",
            },
            {
                "enabled": True,
                "mcp_enabled": True,
                "mcp_server_name": "test-server-2",
                "mcp_server_version": "2.0.0",
                "mcp_transport": "websocket",
            },
            {
                "enabled": False,
                "mcp_enabled": False,
                "mcp_server_name": "disabled-server",
                "mcp_server_version": "1.0.0",
                "mcp_transport": None,
            },
        ]

        for config_dict in test_configs:
            config = McpFeatureServerConfig(**config_dict)
            self.assertEqual(config.enabled, config_dict["enabled"])
            self.assertEqual(config.mcp_enabled, config_dict["mcp_enabled"])
            self.assertEqual(config.mcp_server_name, config_dict["mcp_server_name"])
            self.assertEqual(
                config.mcp_server_version, config_dict["mcp_server_version"]
            )
            self.assertEqual(config.mcp_transport, config_dict["mcp_transport"])
