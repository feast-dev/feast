import os
from abc import ABC, abstractmethod

from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import (
    AuthConfig,
    KubernetesAuthConfig,
    OidcClientAuthConfig,
)


class AuthenticationClientManager(ABC):
    @abstractmethod
    def get_token(self) -> str:
        """Retrieves the token based on the authentication type configuration"""
        pass


class AuthenticationClientManagerFactory(ABC):
    def __init__(self, auth_config: AuthConfig):
        self.auth_config = auth_config

    def get_auth_client_manager(self) -> AuthenticationClientManager:
        from feast.permissions.client.intra_comm_authentication_client_manager import (
            IntraCommAuthClientManager,
        )
        from feast.permissions.client.kubernetes_auth_client_manager import (
            KubernetesAuthClientManager,
        )
        from feast.permissions.client.oidc_authentication_client_manager import (
            OidcAuthClientManager,
        )

        intra_communication_base64 = os.getenv("INTRA_COMMUNICATION_BASE64")
        if intra_communication_base64:
            return IntraCommAuthClientManager(
                self.auth_config, intra_communication_base64
            )

        if self.auth_config.type == AuthType.OIDC.value:
            assert isinstance(self.auth_config, OidcClientAuthConfig)
            return OidcAuthClientManager(self.auth_config)
        elif self.auth_config.type == AuthType.KUBERNETES.value:
            assert isinstance(self.auth_config, KubernetesAuthConfig)
            return KubernetesAuthClientManager(self.auth_config)
        else:
            raise RuntimeError(
                f"No Auth client manager implemented for the auth type:${self.auth_config.type}"
            )
