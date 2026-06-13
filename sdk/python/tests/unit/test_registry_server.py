import logging

import pytest

from feast.permissions.server.utils import AuthManagerType
from feast.registry_server import _warn_if_auth_disabled


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
