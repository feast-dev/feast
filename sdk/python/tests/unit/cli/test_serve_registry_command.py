from unittest.mock import MagicMock

from click.testing import CliRunner

from feast.cli.serve import _serve_rest_registry, serve_registry_command


def _run(mocker, args):
    mock_store = MagicMock()
    mocker.patch("feast.cli.serve.create_feature_store", return_value=mock_store)
    result = CliRunner().invoke(serve_registry_command, args)
    assert result.exit_code == 0, result.output
    return mock_store


def test_help_advertises_dual_stack_default():
    result = CliRunner().invoke(serve_registry_command, ["--help"])
    assert "::" in result.output


def test_rest_only_defaults_host_to_dual_stack(mocker):
    store = _run(mocker, ["--no-grpc", "--rest-api"])
    store.serve_registry.assert_called_once()
    assert store.serve_registry.call_args.kwargs["host"] == "::"


def test_rest_only_host_flag_overrides_default(mocker):
    store = _run(mocker, ["--no-grpc", "--rest-api", "-h", "0.0.0.0"])
    assert store.serve_registry.call_args.kwargs["host"] == "0.0.0.0"


def test_grpc_and_rest_passes_host_to_rest_process(mocker):
    mock_process_cls = mocker.patch("feast.cli.serve.multiprocessing.Process")
    mocker.patch("feast.cli.serve.multiprocessing.set_start_method")
    mock_store = MagicMock(repo_path="/tmp/repo")
    mocker.patch("feast.cli.serve.create_feature_store", return_value=mock_store)

    CliRunner().invoke(serve_registry_command, ["--rest-api", "-h", "0.0.0.0"])

    rest_call = next(
        c
        for c in mock_process_cls.call_args_list
        if c.kwargs["name"] == "rest_registry_server"
    )
    assert rest_call.kwargs["args"][-1] == "0.0.0.0"


def test_serve_rest_registry_forwards_host(mocker):
    mock_store = MagicMock()
    mocker.patch("feast.FeatureStore", return_value=mock_store)

    _serve_rest_registry("/tmp/repo", 6572, "", "", "0.0.0.0")

    mock_store.serve_registry.assert_called_once_with(
        port=6572, tls_key_path="", tls_cert_path="", rest_api=True, host="0.0.0.0"
    )
