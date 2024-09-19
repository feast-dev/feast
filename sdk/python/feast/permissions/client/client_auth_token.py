from feast.permissions.auth_model import (
    AuthConfig,
)
from feast.permissions.client.auth_client_manager import (
    AuthenticationClientManagerFactory,
)


def get_auth_token(auth_config: AuthConfig) -> str:
    return (
        AuthenticationClientManagerFactory(auth_config)
        .get_auth_client_manager()
        .get_token()
    )
