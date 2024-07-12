from feast.permissions.auth_model import AuthConfig
from feast.permissions.client.auth_client_manager import get_auth_client_manager


def create_auth_header(
    auth_config: AuthConfig,
) -> tuple[tuple[str, str]]:
    auth_client_manager = get_auth_client_manager(auth_config)
    token = auth_client_manager.get_token()

    return (("authorization", "Bearer " + token),)
