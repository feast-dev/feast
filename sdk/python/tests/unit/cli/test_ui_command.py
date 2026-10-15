from unittest.mock import MagicMock

from click.testing import CliRunner

from feast.cli.ui import ui


def _run(mocker, args, env=None):
    mock_store = MagicMock()
    mocker.patch("feast.cli.ui.create_feature_store", return_value=mock_store)
    result = CliRunner().invoke(ui, args, obj={}, env=env)
    assert result.exit_code == 0, result.output
    return mock_store


def test_cors_allowed_origins_defaults_to_empty(mocker):
    """Without the flag, no origins are forwarded (CORS stays disabled)."""
    store = _run(mocker, [])
    store.serve_ui.assert_called_once()
    assert store.serve_ui.call_args.kwargs["cors_origins"] == []


def test_cors_allowed_origins_flag_is_split(mocker):
    """The comma-separated flag is parsed into a trimmed list of origins."""
    store = _run(
        mocker,
        [
            "--cors-allowed-origins",
            "https://feast.example.com, https://app.example.com",
        ],
    )
    assert store.serve_ui.call_args.kwargs["cors_origins"] == [
        "https://feast.example.com",
        "https://app.example.com",
    ]


def test_cors_allowed_origins_from_env_var(mocker):
    """FEAST_UI_CORS_ALLOWED_ORIGINS is honored when the flag is absent."""
    store = _run(
        mocker,
        [],
        env={"FEAST_UI_CORS_ALLOWED_ORIGINS": "https://feast.example.com"},
    )
    assert store.serve_ui.call_args.kwargs["cors_origins"] == [
        "https://feast.example.com"
    ]


def test_help_advertises_cors_flag():
    result = CliRunner().invoke(ui, ["--help"])
    assert "--cors-allowed-origins" in result.output
    assert "FEAST_UI_CORS_ALLOWED_ORIGINS" in result.output
