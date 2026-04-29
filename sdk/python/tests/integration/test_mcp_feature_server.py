import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

from fastapi import FastAPI
from pydantic import ValidationError

from feast.feature_store import FeatureStore
from feast.infra.mcp_servers.mcp_config import McpFeatureServerConfig


class TestMCPFeatureServerIntegration(unittest.TestCase):
    """Integration tests for MCP feature server with the MCP SDK."""

    def test_mcp_config_integration(self):
        config = McpFeatureServerConfig(
            enabled=True,
            mcp_enabled=True,
            mcp_server_name="integration-test-server",
            mcp_server_version="2.1.0",
            mcp_transport="sse",
            mcp_base_path="/mcp",
        )

        self.assertTrue(config.enabled)
        self.assertTrue(config.mcp_enabled)
        self.assertEqual(config.mcp_server_name, "integration-test-server")
        self.assertEqual(config.mcp_server_version, "2.1.0")
        self.assertEqual(config.mcp_transport, "sse")
        self.assertEqual(config.mcp_base_path, "/mcp")

    def test_create_mcp_server_with_mock_store(self):
        """Test that create_mcp_server produces a usable FastMCP instance."""
        from feast.infra.mcp_servers.mcp_server import create_mcp_server

        mock_store = MagicMock(spec=FeatureStore)
        mock_store.list_feature_views.return_value = []
        mock_store.list_entities.return_value = []
        mock_store.list_feature_services.return_value = []
        mock_store.list_data_sources.return_value = []

        config = McpFeatureServerConfig(
            mcp_enabled=True,
            mcp_server_name="test-feast-server",
            mcp_server_version="1.0.0",
        )

        mcp = create_mcp_server(mock_store, config)

        self.assertIsNotNone(mcp)
        self.assertEqual(mcp.name, "test-feast-server")

        tool_names = set(mcp._tool_manager._tools.keys())
        self.assertIn("list_feature_views", tool_names)
        self.assertIn("get_online_features", tool_names)
        self.assertIn("retrieve_online_documents", tool_names)

    def test_add_mcp_support_mounts_on_fastapi(self):
        """Test that add_mcp_support_to_app mounts the MCP app on FastAPI."""
        from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

        app = FastAPI()
        mock_store = MagicMock(spec=FeatureStore)

        config = McpFeatureServerConfig(
            enabled=True,
            mcp_enabled=True,
            mcp_server_name="e2e-test-server",
            mcp_server_version="1.0.0",
        )

        result = add_mcp_support_to_app(app, mock_store, config)

        self.assertIsNotNone(result)

        mounted_paths = [route.path for route in app.routes]
        self.assertIn("/mcp", mounted_paths)

    def test_tool_handlers_return_valid_json(self):
        """Test that registry introspection tools return valid JSON."""
        from feast.infra.mcp_servers.mcp_server import create_mcp_server

        mock_store = MagicMock(spec=FeatureStore)

        mock_fv = MagicMock()
        mock_fv.name = "driver_stats"
        mock_fv.entity_columns = []
        mock_fv.features = []
        mock_fv.tags = {"team": "ml"}
        mock_store.list_feature_views.return_value = [mock_fv]

        mock_entity = MagicMock()
        mock_entity.name = "driver_id"
        mock_entity.join_key = "driver_id"
        mock_entity.value_type = MagicMock()
        mock_entity.value_type.name = "INT64"
        mock_entity.tags = {}
        mock_store.list_entities.return_value = [mock_entity]

        mock_store.list_feature_services.return_value = []
        mock_store.list_data_sources.return_value = []

        config = SimpleNamespace(mcp_server_name="json-test")
        mcp = create_mcp_server(mock_store, config)

        for tool_name in [
            "list_feature_views",
            "list_entities",
            "list_feature_services",
            "list_data_sources",
        ]:
            fn = mcp._tool_manager._tools[tool_name].fn
            result = fn(tags=None)
            parsed = json.loads(result)
            self.assertIsInstance(parsed, list)

    def test_feature_server_with_mcp_config(self):
        """Test feature server hook handles MCP config gracefully."""
        from feast.feature_server import _add_mcp_support_if_enabled

        app = FastAPI()
        mock_store = Mock()
        mock_store.config.feature_server = McpFeatureServerConfig(
            enabled=True,
            mcp_enabled=True,
            mcp_server_name="feature-server-test",
            mcp_server_version="1.0.0",
        )

        try:
            _add_mcp_support_if_enabled(app, mock_store)
        except Exception as e:
            self.assertIn("MCP", str(e).upper())

    def test_mcp_server_configuration_validation(self):
        for transport in ["sse", "http"]:
            config = McpFeatureServerConfig(
                enabled=True,
                mcp_enabled=True,
                mcp_server_name="test-server",
                mcp_server_version="1.0.0",
                mcp_transport=transport,
            )
            self.assertEqual(config.mcp_transport, transport)

        with self.assertRaises(ValidationError):
            McpFeatureServerConfig(
                mcp_transport="websocket",
            )
