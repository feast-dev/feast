from unittest.mock import MagicMock, patch

import pytest

from feast.api.registry.rest.rest_registry_server import (
    RestRegistryServer,
    _rest_bind_host,
)
from feast.feature_store import FeatureStore


@pytest.fixture
def mock_store_and_registry():
    mock_registry = MagicMock()
    mock_store = MagicMock(spec=FeatureStore)
    mock_store.registry = mock_registry
    mock_store.project = "test_project"
    mock_config = MagicMock()
    mock_store.config = mock_config
    mock_config.auth_config.type = "no_auth"
    # MagicMock makes nested attrs truthy; keep MCP off unless a test opts in.
    mock_config.registry.mcp = None
    return mock_store, mock_registry


@pytest.mark.xdist_group(name="rest_registry_server")
def test_rest_registry_server_initializes_correctly(
    mock_store_and_registry,
    mocker,
):
    mock_get_auth_manager = mocker.patch(
        "feast.api.registry.rest.rest_registry_server.get_auth_manager"
    )
    mock_init_auth_manager = mocker.patch(
        "feast.api.registry.rest.rest_registry_server.init_auth_manager"
    )
    mock_init_security_manager = mocker.patch(
        "feast.api.registry.rest.rest_registry_server.init_security_manager"
    )
    mock_register_all_routes = mocker.patch(
        "feast.api.registry.rest.rest_registry_server.register_all_routes"
    )
    mock_registry_server_cls = mocker.patch(
        "feast.api.registry.rest.rest_registry_server.RegistryServer"
    )

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

    assert app.include_router.call_count == 16


def test_rest_bind_host_is_ipv6_wildcard_when_available():
    with patch(
        "feast.api.registry.rest.rest_registry_server._ipv6_available",
        return_value=True,
    ):
        assert _rest_bind_host() == "::"


def test_rest_bind_host_falls_back_to_ipv4():
    with patch(
        "feast.api.registry.rest.rest_registry_server._ipv6_available",
        return_value=False,
    ):
        assert _rest_bind_host() == "0.0.0.0"


@pytest.mark.parametrize(
    "ipv6_available,expected_host", [(True, "::"), (False, "0.0.0.0")]
)
@pytest.mark.parametrize("tls", [True, False])
def test_start_server_binds_with_rest_bind_host(
    mock_store_and_registry, mocker, tls, ipv6_available, expected_host
):
    mocker.patch("feast.api.registry.rest.rest_registry_server.get_auth_manager")
    mocker.patch("feast.api.registry.rest.rest_registry_server.init_auth_manager")
    mocker.patch("feast.api.registry.rest.rest_registry_server.init_security_manager")
    mocker.patch("feast.api.registry.rest.rest_registry_server.register_all_routes")
    mocker.patch("feast.api.registry.rest.rest_registry_server.RegistryServer")
    mocker.patch("feast.registry_server._sync_protected_project_tag")
    mock_uvicorn_run = mocker.patch("uvicorn.run")

    store, _ = mock_store_and_registry
    server = RestRegistryServer(store)

    with patch(
        "feast.api.registry.rest.rest_registry_server._ipv6_available",
        return_value=ipv6_available,
    ):
        if tls:
            server.start_server(
                port=6572, tls_key_path="/tmp/key.pem", tls_cert_path="/tmp/cert.pem"
            )
        else:
            server.start_server(port=6572)

    mock_uvicorn_run.assert_called_once()
    _, kwargs = mock_uvicorn_run.call_args
    assert kwargs["host"] == expected_host
    assert kwargs["port"] == 6572
