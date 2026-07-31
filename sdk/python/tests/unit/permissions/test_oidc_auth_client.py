import time
from unittest.mock import MagicMock, patch

import jwt
import pytest
from requests import Session

from feast.permissions.auth_model import (
    KubernetesAuthConfig,
    NoAuthConfig,
    OidcClientAuthConfig,
)
from feast.permissions.client import oidc_authentication_client_manager
from feast.permissions.client.client_auth_token import get_auth_token
from feast.permissions.client.http_auth_requests_wrapper import (
    AuthenticatedRequestsSession,
    get_http_auth_requests_session,
)
from feast.permissions.client.kubernetes_auth_client_manager import (
    KubernetesAuthClientManager,
)
from feast.permissions.client.oidc_authentication_client_manager import (
    OidcAuthClientManager,
)

MOCKED_TOKEN_VALUE: str = "dummy_token"


@pytest.fixture(autouse=True)
def clear_idp_token_cache():
    """The IdP token cache is module-level state; reset it around every test
    so no test inherits (or leaks) a cached token."""
    oidc_authentication_client_manager._token_cache.clear()
    yield
    oidc_authentication_client_manager._token_cache.clear()


def _get_dummy_oidc_auth_type() -> OidcClientAuthConfig:
    oidc_config = OidcClientAuthConfig(
        auth_discovery_url="http://localhost:8080/realms/master/.well-known/openid-configuration",
        type="oidc",
        username="admin_test",
        password="password_test",
        client_id="dummy_client_id",
        client_secret="client_secret",
    )
    return oidc_config


@patch.object(KubernetesAuthClientManager, "get_token", return_value=MOCKED_TOKEN_VALUE)
@patch.object(OidcAuthClientManager, "get_token", return_value=MOCKED_TOKEN_VALUE)
def test_http_auth_requests_session(mock_kubernetes_token, mock_oidc_token):
    no_auth_config = NoAuthConfig()
    assert isinstance(get_http_auth_requests_session(no_auth_config), Session)

    oidc_auth_config = _get_dummy_oidc_auth_type()
    oidc_auth_requests_session = get_http_auth_requests_session(oidc_auth_config)
    _assert_auth_requests_session(oidc_auth_requests_session, MOCKED_TOKEN_VALUE)

    kubernetes_auth_config = KubernetesAuthConfig(type="kubernetes")
    kubernetes_auth_requests_session = get_http_auth_requests_session(
        kubernetes_auth_config
    )
    _assert_auth_requests_session(kubernetes_auth_requests_session, MOCKED_TOKEN_VALUE)


def _assert_auth_requests_session(
    auth_req_session: AuthenticatedRequestsSession, expected_token: str
):
    assert isinstance(auth_req_session, AuthenticatedRequestsSession)
    assert "Authorization" in auth_req_session.headers, (
        "Authorization header is missing in object of class: "
        "AuthenticatedRequestsSession "
    )
    assert auth_req_session.headers["Authorization"] == f"Bearer {expected_token}", (
        "Authorization token is incorrect"
    )


# ---------------------------------------------------------------------------
# IdP token reuse (client_secret / ROPG flow)
# ---------------------------------------------------------------------------

_DISCOVERY = {
    "token_endpoint": "https://idp.example.com/token",
    "authorization_endpoint": "https://idp.example.com/auth",
    "jwks_uri": "https://idp.example.com/jwks",
}


def _jwt_expiring_in(seconds: int) -> str:
    return jwt.encode(
        {"exp": int(time.time()) + seconds}, "test-key", algorithm="HS256"
    )


def _token_response(access_token: str, expires_in=None) -> MagicMock:
    response = MagicMock(status_code=200)
    body = {"access_token": access_token}
    if expires_in is not None:
        body["expires_in"] = expires_in
    response.json.return_value = body
    return response


@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
@patch("feast.permissions.client.oidc_authentication_client_manager.requests.post")
def test_idp_token_reused_until_expiry(mock_post, mock_discovery):
    """One token POST (and one discovery fetch) serves many RPCs: the auth
    interceptors call get_auth_token per outbound request, so without reuse
    every RPC pays two IdP round trips."""
    mock_discovery.return_value = _DISCOVERY
    mock_post.return_value = _token_response(_jwt_expiring_in(3600))

    config = _get_dummy_oidc_auth_type()
    tokens = {get_auth_token(config) for _ in range(5)}

    assert len(tokens) == 1
    assert mock_post.call_count == 1
    assert mock_discovery.call_count == 1


@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
@patch("feast.permissions.client.oidc_authentication_client_manager.requests.post")
def test_token_inside_refresh_margin_is_not_reused(mock_post, mock_discovery):
    """A token whose remaining lifetime is inside the refresh margin must not
    be cached: a reused token must never expire between header injection and
    server-side validation."""
    mock_discovery.return_value = _DISCOVERY
    mock_post.return_value = _token_response(_jwt_expiring_in(5))

    config = _get_dummy_oidc_auth_type()
    get_auth_token(config)
    get_auth_token(config)

    assert mock_post.call_count == 2


@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
@patch("feast.permissions.client.oidc_authentication_client_manager.requests.post")
def test_opaque_token_uses_expires_in_fallback(mock_post, mock_discovery):
    """A non-JWT token is cached when the token endpoint supplies expires_in."""
    mock_discovery.return_value = _DISCOVERY
    mock_post.return_value = _token_response("opaque-token", expires_in=3600)

    config = _get_dummy_oidc_auth_type()
    get_auth_token(config)
    get_auth_token(config)

    assert mock_post.call_count == 1


@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
@patch("feast.permissions.client.oidc_authentication_client_manager.requests.post")
def test_opaque_token_without_expiry_is_not_cached(mock_post, mock_discovery):
    """With no exp claim and no expires_in, expiry is unknowable, so the
    per-call behavior is preserved rather than risking a stale token."""
    mock_discovery.return_value = _DISCOVERY
    mock_post.return_value = _token_response("opaque-token")

    config = _get_dummy_oidc_auth_type()
    get_auth_token(config)
    get_auth_token(config)

    assert mock_post.call_count == 2


@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
@patch("feast.permissions.client.oidc_authentication_client_manager.requests.post")
def test_distinct_configs_do_not_share_tokens(mock_post, mock_discovery):
    """The cache is keyed by the token-request identity: two clients with
    different credentials must never receive each other's tokens."""
    mock_discovery.return_value = _DISCOVERY
    token_a = _jwt_expiring_in(3600)
    token_b = _jwt_expiring_in(7200)
    mock_post.side_effect = [_token_response(token_a), _token_response(token_b)]

    config_a = _get_dummy_oidc_auth_type()
    config_b = _get_dummy_oidc_auth_type()
    config_b.client_id = "another_client_id"

    assert get_auth_token(config_a) == token_a
    assert get_auth_token(config_b) == token_b
    assert get_auth_token(config_a) == token_a
    assert get_auth_token(config_b) == token_b
    assert mock_post.call_count == 2
