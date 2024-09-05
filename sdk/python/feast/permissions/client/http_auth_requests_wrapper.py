import requests
from requests import Session

from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import (
    AuthConfig,
)
from feast.permissions.client.auth_client_manager_factory import get_auth_token


class AuthenticatedRequestsSession(Session):
    def __init__(self, auth_token: str):
        super().__init__()
        self.headers.update({"Authorization": f"Bearer {auth_token}"})


def get_http_auth_requests_session(auth_config: AuthConfig) -> Session:
    if auth_config.type == AuthType.NONE.value:
        request_session = requests.session()
    else:
        request_session = AuthenticatedRequestsSession(get_auth_token(auth_config))
    return request_session
