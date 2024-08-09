import requests
from requests import Session

from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import (
    AuthConfig,
)
from feast.permissions.client.auth_client_manager import get_auth_client_manager


class AuthenticatedRequestsSession(Session):
    def __init__(self, auth_token: str):
        super().__init__()
        self.headers.update({"Authorization": f"Bearer {auth_token}"})


def get_http_auth_requests_session(auth_config: AuthConfig) -> Session:
    if auth_config.type == AuthType.NONE.value:
        request_session = requests.session()
    else:
        auth_client_manager = get_auth_client_manager(auth_config)
        request_session = AuthenticatedRequestsSession(auth_client_manager.get_token())
    return request_session
