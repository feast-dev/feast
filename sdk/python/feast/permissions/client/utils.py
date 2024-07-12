from typing import Optional

from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import AuthConfig
from feast.permissions.client.auth_client_manager import get_auth_client_manager


def create_auth_header(
    auth_config: AuthConfig,
) -> Optional[tuple[tuple[str, str]]]:
    if auth_config.type is AuthType.NONE.value:
        return None

    auth_client_manager = get_auth_client_manager(auth_config)
    token = auth_client_manager.get_token()

    return (("authorization", "Bearer " + token),)
