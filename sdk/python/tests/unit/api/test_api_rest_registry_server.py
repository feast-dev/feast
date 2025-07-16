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
    mock_register_all_routes.assert_called_once_with(server.app, mock_grpc_handler)
    mock_init_security_manager.assert_called_once()
    mock_init_auth_manager.assert_called_once()
    mock_get_auth_manager.assert_called_once()
    assert server.auth_manager == mock_get_auth_manager.return_value

    # OpenAPI security should be injected
    openapi_schema = server.app.openapi()
    assert "securitySchemes" in openapi_schema["components"]
    assert {"BearerAuth": []} in openapi_schema["security"]


def test_routes_registered_in_app(mock_store_and_registry):
    from fastapi.routing import APIRoute

    store, _ = mock_store_and_registry

    server = RestRegistryServer(store)
    route_paths = [
        route.path for route in server.app.routes if isinstance(route, APIRoute)
    ]
    assert "/feature_services" in route_paths
    assert "/entities" in route_paths
    assert "/projects" in route_paths
    assert "/data_sources" in route_paths
    assert "/saved_datasets" in route_paths
    assert "/permissions" in route_paths
    assert "/lineage/registry" in route_paths
    assert "/lineage/objects/{object_type}/{object_name}" in route_paths
    assert "/lineage/complete" in route_paths
    assert "/entities/all" in route_paths
    assert "/feature_views/all" in route_paths
    assert "/data_sources/all" in route_paths
    assert "/feature_services/all" in route_paths
    assert "/saved_datasets/all" in route_paths
    assert "/lineage/registry/all" in route_paths
    assert "/lineage/complete/all" in route_paths
    assert "/features" in route_paths
    assert "/features/all" in route_paths
    assert "/features/{feature_view}/{name}" in route_paths
