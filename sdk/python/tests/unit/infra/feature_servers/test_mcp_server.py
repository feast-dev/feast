import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from pydantic import ValidationError

from feast.feature_store import FeatureStore
from feast.infra.mcp_servers.mcp_config import McpFeatureServerConfig
from feast.permissions.action import AuthzedAction


class TestMcpFeatureServerConfig(unittest.TestCase):
    """Test MCP feature server configuration."""

    def test_default_config(self):
        config = McpFeatureServerConfig()

        self.assertEqual(config.type, "mcp")
        self.assertFalse(config.enabled)
        self.assertFalse(config.mcp_enabled)
        self.assertEqual(config.mcp_server_name, "feast-mcp-server")
        self.assertEqual(config.mcp_server_version, "1.0.0")
        self.assertEqual(config.mcp_transport, "sse")
        self.assertEqual(config.mcp_base_path, "/mcp")
        self.assertEqual(config.transformation_service_endpoint, "localhost:6566")

    def test_custom_config(self):
        config = McpFeatureServerConfig(
            enabled=True,
            mcp_enabled=True,
            mcp_server_name="custom-feast-server",
            mcp_server_version="2.0.0",
            mcp_transport="http",
            mcp_base_path="/custom-mcp",
            transformation_service_endpoint="custom-host:8080",
        )

        self.assertEqual(config.type, "mcp")
        self.assertTrue(config.enabled)
        self.assertTrue(config.mcp_enabled)
        self.assertEqual(config.mcp_server_name, "custom-feast-server")
        self.assertEqual(config.mcp_server_version, "2.0.0")
        self.assertEqual(config.mcp_transport, "http")
        self.assertEqual(config.mcp_base_path, "/custom-mcp")

    def test_config_validation(self):
        for transport in ["sse", "http"]:
            config = McpFeatureServerConfig(mcp_transport=transport)
            self.assertEqual(config.mcp_transport, transport)
        with self.assertRaises(ValidationError):
            McpFeatureServerConfig(mcp_transport="websocket")

    def test_config_inheritance(self):
        config = McpFeatureServerConfig()
        self.assertTrue(hasattr(config, "type"))
        self.assertTrue(hasattr(config, "enabled"))


@patch("feast.infra.mcp_servers.mcp_server.MCP_AVAILABLE", True)
class TestCreateMcpServer(unittest.TestCase):
    """Test create_mcp_server tool registration."""

    def test_creates_fastmcp_instance(self):
        from feast.infra.mcp_servers.mcp_server import create_mcp_server

        mock_store = MagicMock(spec=FeatureStore)
        config = SimpleNamespace(mcp_server_name="test-server")

        mcp = create_mcp_server(mock_store, config)

        self.assertIsNotNone(mcp)
        self.assertEqual(mcp.name, "test-server")

    def test_uses_default_name(self):
        from feast.infra.mcp_servers.mcp_server import create_mcp_server

        mock_store = MagicMock(spec=FeatureStore)
        config = SimpleNamespace()

        mcp = create_mcp_server(mock_store, config)
        self.assertEqual(mcp.name, "feast-feature-store")

    def test_registers_expected_tools(self):
        from feast.infra.mcp_servers.mcp_server import create_mcp_server

        mock_store = MagicMock(spec=FeatureStore)
        config = SimpleNamespace(mcp_server_name="test-server")

        mcp = create_mcp_server(mock_store, config)

        tool_names = set(mcp._tool_manager._tools.keys())
        expected_tools = {
            "list_feature_views",
            "list_entities",
            "list_feature_services",
            "list_data_sources",
            "get_online_features",
            "retrieve_online_documents",
            "write_to_online_store",
            "materialize",
            "materialize_incremental",
        }
        self.assertEqual(tool_names, expected_tools)


@patch("feast.infra.mcp_servers.mcp_server.MCP_AVAILABLE", True)
class TestMcpToolHandlers(unittest.TestCase):
    """Test that MCP tool handlers delegate to FeatureStore correctly."""

    def _create_server(self):
        from feast.infra.mcp_servers.mcp_server import create_mcp_server

        self.mock_store = MagicMock(spec=FeatureStore)
        config = SimpleNamespace(mcp_server_name="test-server")
        return create_mcp_server(self.mock_store, config)

    def _get_tool_fn(self, mcp, name):
        """Get the raw function registered for a tool."""
        return mcp._tool_manager._tools[name].fn

    def test_list_feature_views_delegates(self):
        mcp = self._create_server()
        mock_fv = MagicMock()
        mock_fv.name = "driver_stats"
        mock_fv.entity_columns = []
        mock_fv.features = []
        mock_fv.tags = {}
        self.mock_store.list_feature_views.return_value = [mock_fv]

        fn = self._get_tool_fn(mcp, "list_feature_views")
        result = json.loads(fn(tags=None))

        self.mock_store.list_feature_views.assert_called_once_with(
            allow_cache=True, tags=None
        )
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "driver_stats")

    def test_list_entities_delegates(self):
        mcp = self._create_server()
        mock_entity = MagicMock()
        mock_entity.name = "driver_id"
        mock_entity.join_key = "driver_id"
        mock_entity.value_type = MagicMock()
        mock_entity.value_type.name = "INT64"
        mock_entity.tags = {}
        self.mock_store.list_entities.return_value = [mock_entity]

        fn = self._get_tool_fn(mcp, "list_entities")
        result = json.loads(fn(tags=None))

        self.mock_store.list_entities.assert_called_once_with(
            allow_cache=True, tags=None
        )
        self.assertEqual(result[0]["name"], "driver_id")
        self.assertEqual(result[0]["value_type"], "INT64")

    def test_list_feature_services_delegates(self):
        mcp = self._create_server()
        mock_svc = MagicMock()
        mock_svc.name = "driver_service"
        mock_svc.feature_view_projections = []
        mock_svc.tags = {}
        self.mock_store.list_feature_services.return_value = [mock_svc]

        fn = self._get_tool_fn(mcp, "list_feature_services")
        result = json.loads(fn(tags=None))

        self.mock_store.list_feature_services.assert_called_once_with(tags=None)
        self.assertEqual(result[0]["name"], "driver_service")

    def test_list_data_sources_delegates(self):
        mcp = self._create_server()
        mock_ds = MagicMock()
        mock_ds.name = "driver_source"
        mock_ds.tags = {}
        self.mock_store.list_data_sources.return_value = [mock_ds]

        fn = self._get_tool_fn(mcp, "list_data_sources")
        result = json.loads(fn(tags=None))

        self.mock_store.list_data_sources.assert_called_once_with(
            allow_cache=True, tags=None
        )
        self.assertEqual(result[0]["name"], "driver_source")

    def test_get_online_features_delegates(self):
        mcp = self._create_server()
        mock_response = MagicMock()
        mock_response.to_dict.return_value = {"results": []}
        self.mock_store.get_online_features.return_value = mock_response

        fn = self._get_tool_fn(mcp, "get_online_features")
        result = fn(
            features=["driver_stats:conv_rate"],
            entities={"driver_id": [1001]},
        )

        self.mock_store.get_online_features.assert_called_once_with(
            features=["driver_stats:conv_rate"],
            entity_rows={"driver_id": [1001]},
            full_feature_names=False,
        )
        self.assertIn("results", result)

    def test_retrieve_documents_with_api_version_2_uses_v2(self):
        mcp = self._create_server()
        mock_response = MagicMock()
        mock_response.to_dict.return_value = {"results": []}
        self.mock_store.retrieve_online_documents_v2.return_value = mock_response

        fn = self._get_tool_fn(mcp, "retrieve_online_documents")
        fn(features=["doc_fv:embedding"], query_string="hello", top_k=5, api_version=2)

        self.mock_store.retrieve_online_documents_v2.assert_called_once()
        self.mock_store.retrieve_online_documents.assert_not_called()

    def test_retrieve_documents_with_api_version_1_uses_v1(self):
        mcp = self._create_server()
        mock_response = MagicMock()
        mock_response.to_dict.return_value = {"results": []}
        self.mock_store.retrieve_online_documents.return_value = mock_response

        fn = self._get_tool_fn(mcp, "retrieve_online_documents")
        fn(features=["doc_fv:embedding"], query=[0.1, 0.2], top_k=5, api_version=1)

        self.mock_store.retrieve_online_documents.assert_called_once()
        self.mock_store.retrieve_online_documents_v2.assert_not_called()

    def test_retrieve_documents_defaults_to_v1(self):
        mcp = self._create_server()
        mock_response = MagicMock()
        mock_response.to_dict.return_value = {"results": []}
        self.mock_store.retrieve_online_documents.return_value = mock_response

        fn = self._get_tool_fn(mcp, "retrieve_online_documents")
        fn(features=["doc_fv:embedding"], query=[0.1, 0.2], top_k=5)

        self.mock_store.retrieve_online_documents.assert_called_once()
        self.mock_store.retrieve_online_documents_v2.assert_not_called()


@patch("feast.infra.mcp_servers.mcp_server.MCP_AVAILABLE", True)
class TestMcpToolAuthorizationChecks(unittest.TestCase):
    """Test that MCP tool handlers enforce authorization checks."""

    def _create_server(self):
        from feast.infra.mcp_servers.mcp_server import create_mcp_server

        self.mock_store = MagicMock(spec=FeatureStore)
        config = SimpleNamespace(mcp_server_name="test-server")
        return create_mcp_server(self.mock_store, config)

    def _get_tool_fn(self, mcp, name):
        return mcp._tool_manager._tools[name].fn

    @patch("feast.infra.mcp_servers.mcp_server.assert_permissions")
    @patch("feast.infra.mcp_servers.mcp_server.feast_utils._get_feature_views_to_use")
    def test_get_online_features_checks_read_permissions(
        self, mock_get_fvs, mock_assert
    ):
        mcp = self._create_server()
        mock_fv = MagicMock()
        mock_get_fvs.return_value = ([mock_fv], [])
        mock_response = MagicMock()
        mock_response.to_dict.return_value = {}
        self.mock_store.get_online_features.return_value = mock_response

        fn = self._get_tool_fn(mcp, "get_online_features")
        fn(features=["fv:feat"], entities={"id": [1]})

        mock_assert.assert_called()
        call_kwargs = mock_assert.call_args_list[0]
        self.assertEqual(call_kwargs.kwargs["actions"], [AuthzedAction.READ_ONLINE])

    @patch("feast.infra.mcp_servers.mcp_server.assert_permissions")
    @patch("feast.infra.mcp_servers.mcp_server.feast_utils._get_feature_views_to_use")
    def test_retrieve_documents_checks_read_permissions(
        self, mock_get_fvs, mock_assert
    ):
        mcp = self._create_server()
        mock_fv = MagicMock()
        mock_get_fvs.return_value = ([mock_fv], [])
        mock_response = MagicMock()
        mock_response.to_dict.return_value = {}
        self.mock_store.retrieve_online_documents_v2.return_value = mock_response

        fn = self._get_tool_fn(mcp, "retrieve_online_documents")
        fn(features=["fv:feat"], query_string="test", top_k=3, api_version=2)

        mock_assert.assert_called()
        call_kwargs = mock_assert.call_args_list[0]
        self.assertEqual(call_kwargs.kwargs["actions"], [AuthzedAction.READ_ONLINE])

    @patch("feast.infra.mcp_servers.mcp_server.assert_permissions")
    @patch("feast.infra.mcp_servers.mcp_server.get_feature_view_from_feature_store")
    def test_write_to_online_store_checks_write_permissions(
        self, mock_get_fv, mock_assert
    ):
        mcp = self._create_server()
        mock_fv = MagicMock()
        mock_get_fv.return_value = mock_fv

        fn = self._get_tool_fn(mcp, "write_to_online_store")
        fn(feature_view_name="my_fv", df={"id": ["a"], "val": [1]})

        mock_assert.assert_called_once_with(
            resource=mock_fv, actions=[AuthzedAction.WRITE_ONLINE]
        )

    @patch("feast.infra.mcp_servers.mcp_server.assert_permissions")
    @patch("feast.infra.mcp_servers.mcp_server.get_feature_view_from_feature_store")
    def test_materialize_checks_write_permissions(self, mock_get_fv, mock_assert):
        mcp = self._create_server()
        mock_fv = MagicMock()
        mock_get_fv.return_value = mock_fv

        fn = self._get_tool_fn(mcp, "materialize")
        fn(
            start_ts="2024-01-01T00:00:00",
            end_ts="2024-01-02T00:00:00",
            feature_views=["fv1", "fv2"],
        )

        self.assertEqual(mock_assert.call_count, 2)
        for call in mock_assert.call_args_list:
            self.assertEqual(call.kwargs["actions"], [AuthzedAction.WRITE_ONLINE])

    @patch("feast.infra.mcp_servers.mcp_server.assert_permissions")
    @patch("feast.infra.mcp_servers.mcp_server.get_feature_view_from_feature_store")
    def test_materialize_incremental_checks_write_permissions(
        self, mock_get_fv, mock_assert
    ):
        mcp = self._create_server()
        mock_fv = MagicMock()
        mock_get_fv.return_value = mock_fv

        fn = self._get_tool_fn(mcp, "materialize_incremental")
        fn(end_ts="2024-01-02T00:00:00", feature_views=["fv1"])

        mock_assert.assert_called_once_with(
            resource=mock_fv, actions=[AuthzedAction.WRITE_ONLINE]
        )

    @patch("feast.infra.mcp_servers.mcp_server.assert_permissions")
    @patch("feast.infra.mcp_servers.mcp_server.get_feature_view_from_feature_store")
    def test_write_denied_blocks_store_call(self, mock_get_fv, mock_assert):
        from feast.errors import FeastPermissionError

        mcp = self._create_server()
        mock_get_fv.return_value = MagicMock()
        mock_assert.side_effect = FeastPermissionError("denied")

        fn = self._get_tool_fn(mcp, "write_to_online_store")
        with self.assertRaises(FeastPermissionError):
            fn(feature_view_name="my_fv", df={"id": ["a"], "val": [1]})

        self.mock_store.write_to_online_store.assert_not_called()


@patch("feast.infra.mcp_servers.mcp_server.MCP_AVAILABLE", True)
class TestAddMcpSupportToApp(unittest.TestCase):
    """Test add_mcp_support_to_app mounting logic."""

    def test_mounts_sse_app(self):
        from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

        mock_app = MagicMock()
        mock_store = MagicMock(spec=FeatureStore)
        config = SimpleNamespace(
            mcp_server_name="test-server",
            mcp_server_version="1.0.0",
            mcp_transport="sse",
            mcp_base_path="/mcp",
        )

        result = add_mcp_support_to_app(mock_app, mock_store, config)

        self.assertIsNotNone(result)
        mock_app.mount.assert_called_once()
        call_args = mock_app.mount.call_args
        self.assertEqual(call_args[0][0], "/mcp")

    def test_mounts_http_app(self):
        from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

        mock_app = MagicMock()
        mock_store = MagicMock(spec=FeatureStore)
        config = SimpleNamespace(
            mcp_server_name="test-server",
            mcp_server_version="1.0.0",
            mcp_transport="http",
            mcp_base_path="/mcp",
        )

        result = add_mcp_support_to_app(mock_app, mock_store, config)

        self.assertIsNotNone(result)
        mock_app.mount.assert_called_once()
        call_args = mock_app.mount.call_args
        self.assertEqual(call_args[0][0], "/mcp")

    def test_invalid_transport_raises(self):
        from feast.infra.mcp_servers.mcp_server import (
            McpTransportNotSupportedError,
            add_mcp_support_to_app,
        )

        mock_app = MagicMock()
        mock_store = MagicMock(spec=FeatureStore)
        config = SimpleNamespace(
            mcp_server_name="test-server",
            mcp_transport="grpc",
        )

        with self.assertRaises(McpTransportNotSupportedError):
            add_mcp_support_to_app(mock_app, mock_store, config)

    def test_custom_base_path(self):
        from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

        mock_app = MagicMock()
        mock_store = MagicMock(spec=FeatureStore)
        config = SimpleNamespace(
            mcp_server_name="test-server",
            mcp_server_version="1.0.0",
            mcp_transport="sse",
            mcp_base_path="/custom-mcp",
        )

        add_mcp_support_to_app(mock_app, mock_store, config)

        call_args = mock_app.mount.call_args
        self.assertEqual(call_args[0][0], "/custom-mcp")


@patch("feast.infra.mcp_servers.mcp_server.MCP_AVAILABLE", False)
class TestMCPNotAvailable(unittest.TestCase):
    """Test behavior when MCP SDK is not available."""

    @patch("feast.infra.mcp_servers.mcp_server.logger")
    def test_add_mcp_support_mcp_not_available(self, mock_logger):
        from feast.infra.mcp_servers.mcp_server import add_mcp_support_to_app

        result = add_mcp_support_to_app(Mock(), Mock(), Mock())

        self.assertIsNone(result)
        mock_logger.warning.assert_called_once_with(
            "MCP support requested but mcp SDK is not available"
        )


class TestFeatureServerMCPHooks(unittest.TestCase):
    """Test MCP hooks in feature server."""

    @patch("feast.feature_server.logger")
    def test_add_mcp_support_if_enabled_exception(self, mock_logger):
        from feast.feature_server import _add_mcp_support_if_enabled

        mock_app = Mock()
        mock_store = Mock()
        mock_store.config.feature_server = Mock()
        mock_store.config.feature_server.type = "mcp"
        mock_store.config.feature_server.mcp_enabled = True

        with patch(
            "feast.infra.mcp_servers.mcp_server.add_mcp_support_to_app",
            side_effect=Exception("Test error"),
        ):
            _add_mcp_support_if_enabled(mock_app, mock_store)

            mock_logger.error.assert_called_with(
                "Error checking/adding MCP support: Test error"
            )

    @patch("feast.infra.mcp_servers.mcp_server.add_mcp_support_to_app")
    def test_add_mcp_support_if_enabled_transport_not_supported_fails(self, mock_add):
        from feast.feature_server import _add_mcp_support_if_enabled
        from feast.infra.mcp_servers.mcp_server import McpTransportNotSupportedError

        mock_app = Mock()
        mock_store = Mock()
        mock_store.config.feature_server = Mock()
        mock_store.config.feature_server.type = "mcp"
        mock_store.config.feature_server.mcp_enabled = True
        mock_store.config.feature_server.mcp_transport = "http"

        mock_add.side_effect = McpTransportNotSupportedError("bad")

        with self.assertRaises(McpTransportNotSupportedError):
            _add_mcp_support_if_enabled(mock_app, mock_store)
