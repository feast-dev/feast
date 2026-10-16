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


def _patch_start_server_collaborators(mocker):
    """Patch both branches' collaborators so a comparison mutant can't hang on a real call."""
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
    mock_run = mocker.patch("uvicorn.run")
    return {
        "make_dual_stack_socket": mock_make_dual_stack_socket,
        "sock": mock_sock,
        "config_cls": mock_config_cls,
        "server_cls": mock_server_cls,
        "server_instance": mock_server_instance,
        "run": mock_run,
    }


@pytest.mark.parametrize("tls", [True, False])
def test_start_server_wires_uvicorn_server_with_prebound_socket(
    mock_store_and_registry, mocker, tls, caplog
):
    mocks = _patch_start_server_collaborators(mocker)
    caplog.set_level("INFO")

    store, _ = mock_store_and_registry
    server = RestRegistryServer(store)

    if tls:
        server.start_server(
            port=6572, tls_key_path="/tmp/key.pem", tls_cert_path="/tmp/cert.pem"
        )
    else:
        server.start_server(port=6572)

    mocks["make_dual_stack_socket"].assert_called_once_with(6572)
    mocks["server_cls"].assert_called_once_with(mocks["config_cls"].return_value)
    mocks["server_instance"].run.assert_called_once_with(sockets=[mocks["sock"]])
    mocks["run"].assert_not_called()

    _, config_kwargs = mocks["config_cls"].call_args
    if tls:
        assert config_kwargs["ssl_keyfile"] == "/tmp/key.pem"
        assert config_kwargs["ssl_certfile"] == "/tmp/cert.pem"
        assert "in TLS(SSL) mode" in caplog.text
        assert "in non-TLS(SSL) mode" not in caplog.text
    else:
        assert "ssl_keyfile" not in config_kwargs
        assert "in non-TLS(SSL) mode" in caplog.text


def test_start_server_treats_runtime_equal_host_as_dual_stack(
    mock_store_and_registry, mocker
):
    """A "::" value built at runtime (not the literal's interned object) must
    still take the dual-stack path -- kills an `==` -> `is` mutant."""
    mocks = _patch_start_server_collaborators(mocker)

    store, _ = mock_store_and_registry
    server = RestRegistryServer(store)

    sep = ":"
    host = sep + sep

    server.start_server(port=6572, host=host)

    mocks["make_dual_stack_socket"].assert_called_once_with(6572)
    mocks["run"].assert_not_called()


@pytest.mark.parametrize("tls", [True, False])
def test_start_server_non_dual_stack_host_uses_uvicorn_run(
    mock_store_and_registry, mocker, tls, caplog
):
    """host="0.0.0.0" keeps the plain uvicorn.run path."""
    mocks = _patch_start_server_collaborators(mocker)
    caplog.set_level("INFO")

    store, _ = mock_store_and_registry
    server = RestRegistryServer(store)

    ssl_kwargs = (
        {"tls_key_path": "/tmp/key.pem", "tls_cert_path": "/tmp/cert.pem"}
        if tls
        else {}
    )
    server.start_server(port=6572, host="0.0.0.0", **ssl_kwargs)

    mocks["server_cls"].assert_not_called()
    mocks["make_dual_stack_socket"].assert_not_called()
    if tls:
        mocks["run"].assert_called_once_with(
            server.app,
            host="0.0.0.0",
            port=6572,
            ssl_keyfile="/tmp/key.pem",
            ssl_certfile="/tmp/cert.pem",
        )
        assert "in TLS(SSL) mode" in caplog.text
        assert "in non-TLS(SSL) mode" not in caplog.text
    else:
        mocks["run"].assert_called_once_with(server.app, host="0.0.0.0", port=6572)
        assert "in non-TLS(SSL) mode" in caplog.text


def test_start_server_non_wildcard_host_uses_uvicorn_run(
    mock_store_and_registry, mocker
):
    # "example.com" sorts after "::"; kills a `==` -> `>=`/`<=` mutant that a
    # "0.0.0.0"-only test (which sorts before "::" either way) can't catch.
    mocks = _patch_start_server_collaborators(mocker)

    store, _ = mock_store_and_registry
    server = RestRegistryServer(store)

    server.start_server(port=6572, host="example.com")

    mocks["run"].assert_called_once_with(server.app, host="example.com", port=6572)
    mocks["server_cls"].assert_not_called()
    mocks["make_dual_stack_socket"].assert_not_called()
