import requests
from requests import Session

from feast.permissions.auth_model import (
    AuthConfig,
    KubernetesAuthConfig,
    OidcAuthConfig,
)
from feast.permissions.client.kubernetes_auth_client_manager import (
    KubernetesAuthClientManager,
)
from feast.permissions.client.oidc_authentication_client_manager import (
    OidcAuthClientManager,
)


class AuthenticatedRequestsSession(Session):
    def __init__(self, auth_token: str):
        super().__init__()
        self.auth_token = auth_token
        self.headers.update({"Authorization": f"Bearer {self.auth_token}"})


def get_auth_client_manager(auth_config: AuthConfig):
    if auth_config.type == "oidc":
        assert isinstance(auth_config, OidcAuthConfig)
        return OidcAuthClientManager(auth_config)
    elif auth_config.type == "kubernetes":
        assert isinstance(auth_config, KubernetesAuthConfig)
        return KubernetesAuthClientManager(auth_config)
    else:
        raise RuntimeError(
            f"No Auth client manager implemented for the auth type:${auth_config.type}"
        )


def get_http_auth_requests_session(auth_config: AuthConfig) -> Session:
    if auth_config.type == "no_auth":
        request_session = requests.session()
    else:
        auth_client_manager = get_auth_client_manager(auth_config)
        request_session = AuthenticatedRequestsSession(auth_client_manager.get_token())
    return request_session
