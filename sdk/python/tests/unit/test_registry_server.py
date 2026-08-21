import logging
from types import SimpleNamespace

import pytest

from feast.permissions.server.utils import AuthManagerType
from feast.registry_server import _warn_if_auth_disabled, start_server


def test_warn_if_auth_disabled_emits_warning_for_none(caplog):
    with caplog.at_level(logging.WARNING, logger="feast.registry_server"):
        _warn_if_auth_disabled(AuthManagerType.NONE)
    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.WARNING
    assert "no_auth" in caplog.records[0].message


def test_warn_if_auth_disabled_no_warning_for_oidc(caplog):
    with caplog.at_level(logging.WARNING, logger="feast.registry_server"):
        _warn_if_auth_disabled(AuthManagerType.OIDC)
    assert len(caplog.records) == 0


def test_start_server_warns_when_auth_disabled(caplog, monkeypatch):
    class Sentinel(Exception):
        pass

    store = SimpleNamespace(
        config=SimpleNamespace(
            auth_config=SimpleNamespace(type=AuthManagerType.NONE.value)
        )
    )

    def fail_after_warning(auth_type, fs):
        assert auth_type == AuthManagerType.NONE
        assert fs is store
        assert any(
            record.levelno == logging.WARNING and "no_auth" in record.message
            for record in caplog.records
        )
        raise Sentinel

    monkeypatch.setattr(
        "feast.registry_server.init_security_manager", fail_after_warning
    )

    with caplog.at_level(logging.WARNING, logger="feast.registry_server"):
        with pytest.raises(Sentinel):
            start_server(store, port=0)

    assert len(caplog.records) == 1
    assert caplog.records[0].levelno == logging.WARNING
    assert "no_auth" in caplog.records[0].message
