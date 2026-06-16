from unittest.mock import MagicMock, patch

import pytest

from feast.api.registry.rest.rest_registry_server import RestRegistryServer
from feast.feature_store import FeatureStore


@pytest.fixture
def mock_store_and_registry():
    mock_registry = MagicMock()
    mock_store = MagicMock(spec=FeatureStore)
    mock_store.registry = mock_registry
    mock_store.config = MagicMock()
    return mock_store, mock_registry


@patch("feast.api.registry.rest.rest_registry_server.RegistryServer")
@patch("feast.api.registry.rest.rest_registry_server.register_all_routes")
@patch("feast.api.registry.rest.rest_registry_server.init_security_manager")
@patch("feast.api.registry.rest.rest_registry_server.init_auth_manager")
@patch("feast.api.registry.rest.rest_registry_server.get_auth_manager")
def test_rest_registry_server_initializes_correctly(
    mock_get_auth_manager,
    mock_init_auth_manager,
    mock_init_security_manager,
    mock_register_all_routes,
    mock_registry_server_cls,
    mock_store_and_registry,
):
    store, registry = mock_store_and_registry
    mock_grpc_handler = MagicMock()
    mock_registry_server_cls.return_value = mock_grpc_handler

    server = RestRegistryServer(store)

    # Validate registry and grpc handler are wired
    assert server.store == store
    assert server.registry == registry
    assert server.grpc_handler == mock_grpc_handler

    # Validate route registration and auth init
    mock_register_all_routes.assert_called_once_with(
        server.app, mock_grpc_handler, server
    )
    mock_init_security_manager.assert_called_once()
    mock_init_auth_manager.assert_called_once()
    mock_get_auth_manager.assert_called_once()
    assert server.auth_manager == mock_get_auth_manager.return_value

    # OpenAPI security should be injected
    openapi_schema = server.app.openapi()
    assert "securitySchemes" in openapi_schema["components"]
    assert {"BearerAuth": []} in openapi_schema["security"]


def test_routes_registered_in_app():
    from feast.api.registry.rest import register_all_routes

    app = MagicMock()
    grpc_handler = MagicMock()
    server = MagicMock()
    register_all_routes(app, grpc_handler, server)

    assert app.include_router.call_count == 13
