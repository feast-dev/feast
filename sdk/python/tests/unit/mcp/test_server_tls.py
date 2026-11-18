"""Serving the standalone MCP server's HTTP transports over HTTPS."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import click
import pytest
from gunicorn.app.base import BaseApplication

from feast.mcp.server import _run_gunicorn, _run_uvicorn, run_server

CERT = "/certs/tls.crt"
KEY = "/certs/tls.key"


def _cfg(workers=None):
    return SimpleNamespace(
        server=SimpleNamespace(
            host="0.0.0.0", port=8443, workers=workers, tls_cert=CERT, tls_key=KEY
        )
    )


@patch("feast.mcp.server._build_http_app")
@patch("uvicorn.run")
def test_uvicorn_serves_https(mock_run, mock_app):
    _run_uvicorn(MagicMock(), _cfg())

    mock_run.assert_called_once_with(
        mock_app.return_value,
        host="0.0.0.0",
        port=8443,
        ssl_certfile=CERT,
        ssl_keyfile=KEY,
    )


@patch("feast.mcp.server._build_http_app")
def test_gunicorn_serves_https(_mock_app):
    with patch.object(BaseApplication, "run", autospec=True) as run:
        _run_gunicorn(MagicMock(), _cfg(workers=2))

    app = run.call_args.args[0]
    assert app.cfg.certfile == CERT
    assert app.cfg.keyfile == KEY


def test_cert_without_key_is_rejected(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)  # no stray feast_mcp.yaml
    for name in ("FEAST_MCP_TLS_CERT", "FEAST_MCP_TLS_KEY"):
        monkeypatch.delenv(name, raising=False)

    with (
        patch("feast.mcp.server.configure_logging"),
        patch("feast.mcp.server.create_mcp") as create_mcp,
        pytest.raises(click.UsageError, match="--tls-cert and --tls-key"),
    ):
        run_server(cli_args={"feast_url": "http://feast", "tls_cert": CERT})
    create_mcp.assert_not_called()
