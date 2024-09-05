from unittest.mock import patch

from requests import Session

from feast.permissions.auth_model import (
    KubernetesAuthConfig,
    NoAuthConfig,
    OidcClientAuthConfig,
)
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
    assert (
        auth_req_session.headers["Authorization"] == f"Bearer {expected_token}"
    ), "Authorization token is incorrect"
