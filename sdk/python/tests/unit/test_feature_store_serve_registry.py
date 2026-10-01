from unittest.mock import MagicMock

from feast.feature_store import FeatureStore


def test_serve_registry_forwards_host_to_rest_server(mocker):
    mock_server_cls = mocker.patch(
        "feast.api.registry.rest.rest_registry_server.RestRegistryServer"
    )
    mock_store = MagicMock()

    FeatureStore.serve_registry(mock_store, 6572, rest_api=True, host="0.0.0.0")

    mock_server_cls.return_value.start_server.assert_called_once_with(
        port=6572, host="0.0.0.0", tls_key_path="", tls_cert_path=""
    )


def test_serve_registry_defaults_host_to_dual_stack(mocker):
    mock_server_cls = mocker.patch(
        "feast.api.registry.rest.rest_registry_server.RestRegistryServer"
    )
    mock_store = MagicMock()

    FeatureStore.serve_registry(mock_store, 6572, rest_api=True)

    assert mock_server_cls.return_value.start_server.call_args.kwargs["host"] == "::"
