"""IPv6 / dual-stack binds for the standalone MCP server (host ``::``)."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from feast.mcp.server import _gunicorn_bind, _run_uvicorn


def _cfg(host: str, port: int = 8000):
    return SimpleNamespace(server=SimpleNamespace(host=host, port=port))


@patch("feast.mcp.server._build_http_app")
@patch("uvicorn.run")
@patch("uvicorn.Server")
@patch("uvicorn.Config")
def test_uvicorn_dual_stack_binds_prebuilt_socket(
    mock_config, mock_server, mock_run, _mock_app
):
    """uvicorn.run(host="::") binds IPv6-only and drops IPv4 clients."""
    sock = MagicMock()
    with patch("feast.utils._make_dual_stack_socket", return_value=sock) as make:
        _run_uvicorn(MagicMock(), _cfg("::"))

    make.assert_called_once_with(8000)
    mock_server.return_value.run.assert_called_once_with(sockets=[sock])
    mock_run.assert_not_called()


@patch("feast.mcp.server._build_http_app")
@patch("uvicorn.run")
def test_uvicorn_other_hosts_use_plain_run(mock_run, mock_app):
    _run_uvicorn(MagicMock(), _cfg("0.0.0.0"))

    mock_run.assert_called_once_with(mock_app.return_value, host="0.0.0.0", port=8000)


@pytest.mark.parametrize(
    "host, ipv6, want",
    [
        ("::", True, "[::]:8000"),
        ("::", False, "0.0.0.0:8000"),
        ("0.0.0.0", True, "0.0.0.0:8000"),
        ("127.0.0.1", False, "127.0.0.1:8000"),
    ],
)
def test_gunicorn_bind(host, ipv6, want):
    with patch("feast.utils._ipv6_available", return_value=ipv6):
        assert _gunicorn_bind(_cfg(host)) == want
