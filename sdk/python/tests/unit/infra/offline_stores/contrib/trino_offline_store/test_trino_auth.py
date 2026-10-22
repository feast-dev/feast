from pydantic import SecretStr
from trino.auth import BasicAuthentication, JWTAuthentication, OAuth2Authentication

from feast.infra.offline_stores.contrib.trino_offline_store.trino import (
    CLASSES_BY_AUTH_TYPE,
    AuthConfig,
    FeastConfigBaseModel,
)


def test_jwt_auth_produces_plain_str_token():
    auth = AuthConfig(
        type="jwt",
        config={"token": "my-secret-token"},  # pragma: allowlist secret
    )

    trino_auth = auth.to_trino_auth()

    assert isinstance(trino_auth, JWTAuthentication)
    assert trino_auth.token == "my-secret-token"  # pragma: allowlist secret
    assert isinstance(trino_auth.token, str)


def test_oauth2_auth_unchanged():
    auth = AuthConfig(type="oauth2", config=None)

    trino_auth = auth.to_trino_auth()

    assert isinstance(trino_auth, OAuth2Authentication)


def test_basic_auth_with_plain_fields_unaffected():
    auth = AuthConfig(
        type="basic",
        config={"username": "alice", "password": "hunter2"},  # pragma: allowlist secret
    )

    trino_auth = auth.to_trino_auth()

    assert isinstance(trino_auth, BasicAuthentication)
    assert trino_auth._username == "alice"
    assert trino_auth._password == "hunter2"  # pragma: allowlist secret


class _MixedAuthModel(FeastConfigBaseModel):
    username: str
    token: SecretStr


class _MixedAuth:
    def __init__(self, username: str, token: str):
        self.username = username
        self.token = token


def test_to_trino_auth_unwraps_only_secret_fields_in_mixed_model(monkeypatch):
    monkeypatch.setitem(
        CLASSES_BY_AUTH_TYPE,
        "jwt",
        {"auth_model": _MixedAuthModel, "trino_auth": _MixedAuth},
    )
    auth = AuthConfig(
        type="jwt",
        config={
            "username": "alice",
            "token": "my-secret-token",  # pragma: allowlist secret
        },
    )

    trino_auth = auth.to_trino_auth()

    assert trino_auth.username == "alice"
    assert trino_auth.token == "my-secret-token"  # pragma: allowlist secret
    assert isinstance(trino_auth.username, str)
    assert isinstance(trino_auth.token, str)
