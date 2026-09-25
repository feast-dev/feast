from unittest.mock import MagicMock

import pytest

from feast.api.registry.rest.rest_registry_server import RestRegistryServer
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


@pytest.mark.parametrize("tls", [True, False])
def test_start_server_wires_uvicorn_server_with_prebound_socket(
    mock_store_and_registry, mocker, tls
):
    mocker.patch("feast.api.registry.rest.rest_registry_server.get_auth_manager")
    mocker.patch("feast.api.registry.rest.rest_registry_server.init_auth_manager")
    mocker.patch("feast.api.registry.rest.rest_registry_server.init_security_manager")
    mocker.patch("feast.api.registry.rest.rest_registry_server.register_all_routes")
    mocker.patch("feast.api.registry.rest.rest_registry_server.RegistryServer")
    mocker.patch("feast.registry_server._sync_protected_project_tag")

    mock_sock = MagicMock()
    mock_make_dual_stack_socket = mocker.patch(
        "feast.api.registry.rest.rest_registry_server._make_dual_stack_socket",
        return_value=mock_sock,
    )
    mock_config_cls = mocker.patch("uvicorn.Config")
    mock_server_cls = mocker.patch("uvicorn.Server")
    mock_server_instance = MagicMock()
    mock_server_cls.return_value = mock_server_instance

    store, _ = mock_store_and_registry
    server = RestRegistryServer(store)

    if tls:
        server.start_server(
            port=6572, tls_key_path="/tmp/key.pem", tls_cert_path="/tmp/cert.pem"
        )
    else:
        server.start_server(port=6572)

    mock_make_dual_stack_socket.assert_called_once_with(6572)
    mock_server_cls.assert_called_once_with(mock_config_cls.return_value)
    mock_server_instance.run.assert_called_once_with(sockets=[mock_sock])

    _, config_kwargs = mock_config_cls.call_args
    if tls:
        assert config_kwargs["ssl_keyfile"] == "/tmp/key.pem"
        assert config_kwargs["ssl_certfile"] == "/tmp/cert.pem"
    else:
        assert "ssl_keyfile" not in config_kwargs
