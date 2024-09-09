import os
from typing import cast

from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import (
    AuthConfig,
    KubernetesAuthConfig,
    OidcAuthConfig,
    OidcClientAuthConfig,
)
from feast.permissions.client.auth_client_manager import AuthenticationClientManager
from feast.permissions.client.kubernetes_auth_client_manager import (
    KubernetesAuthClientManager,
)
from feast.permissions.client.oidc_authentication_client_manager import (
    OidcAuthClientManager,
)


def get_auth_client_manager(auth_config: AuthConfig) -> AuthenticationClientManager:
    if auth_config.type == AuthType.OIDC.value:
        intra_communication_base64 = os.getenv("INTRA_COMMUNICATION_BASE64")
        # If intra server communication call
        if intra_communication_base64:
            assert isinstance(auth_config, OidcAuthConfig)
            client_auth_config = cast(OidcClientAuthConfig, auth_config)
        else:
            assert isinstance(auth_config, OidcClientAuthConfig)
            client_auth_config = auth_config
        return OidcAuthClientManager(client_auth_config)
    elif auth_config.type == AuthType.KUBERNETES.value:
        assert isinstance(auth_config, KubernetesAuthConfig)
        return KubernetesAuthClientManager(auth_config)
    else:
        raise RuntimeError(
            f"No Auth client manager implemented for the auth type:${auth_config.type}"
        )


def get_auth_token(auth_config: AuthConfig) -> str:
    return get_auth_client_manager(auth_config).get_token()
