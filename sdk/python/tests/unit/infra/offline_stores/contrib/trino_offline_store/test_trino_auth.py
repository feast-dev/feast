from trino.auth import JWTAuthentication, OAuth2Authentication

from feast.infra.offline_stores.contrib.trino_offline_store.trino import AuthConfig


def test_jwt_auth_produces_plain_str_token():
    auth = AuthConfig(type="jwt", config={"token": "my-secret-token"})

    trino_auth = auth.to_trino_auth()

    assert isinstance(trino_auth, JWTAuthentication)
    assert trino_auth.token == "my-secret-token"
    assert isinstance(trino_auth.token, str)


def test_oauth2_auth_unchanged():
    auth = AuthConfig(type="oauth2", config=None)

    trino_auth = auth.to_trino_auth()

    assert isinstance(trino_auth, OAuth2Authentication)
