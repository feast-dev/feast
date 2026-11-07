import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="gunicorn is not used on Windows"
)


def _start_server(feature_server_config, gc_freeze: bool):
    """Run start_server with the gunicorn app mocked; return the call order."""
    from feast.feature_server import start_server

    calls = MagicMock()
    store = MagicMock()
    store.config = SimpleNamespace(
        feature_server=feature_server_config, auth_config=MagicMock()
    )
    application = MagicMock()
    application.run.side_effect = lambda: calls.run()
    with (
        patch("feast.feature_server.feast_metrics"),
        patch("feast.feature_server.str_to_auth_manager_type"),
        patch("feast.feature_server.init_security_manager"),
        patch("feast.feature_server.init_auth_manager"),
        patch("feast.feature_server.FeastServeApplication", return_value=application),
        patch("feast.feature_server.gc.collect", side_effect=calls.collect),
        patch("feast.feature_server.gc.freeze", side_effect=calls.freeze),
    ):
        start_server(
            store=store,
            host="127.0.0.1",
            port=6566,
            no_access_log=True,
            workers=1,
            worker_connections=1000,
            max_requests=1000,
            max_requests_jitter=50,
            keep_alive_timeout=30,
            registry_ttl_sec=60,
            tls_key_path="",
            tls_cert_path="",
            metrics=False,
            gc_freeze=gc_freeze,
        )
    return [name for name, _, _ in calls.mock_calls]


def test_cli_flag_freezes_before_workers_start():
    assert _start_server(None, gc_freeze=True) == ["collect", "freeze", "run"]


def test_config_enables_freeze():
    config = SimpleNamespace(metrics=None, gc_freeze=True)
    assert _start_server(config, gc_freeze=False) == ["collect", "freeze", "run"]


def test_no_freeze_by_default():
    config = SimpleNamespace(metrics=None, gc_freeze=False)
    assert _start_server(config, gc_freeze=False) == ["run"]
    assert _start_server(None, gc_freeze=False) == ["run"]
