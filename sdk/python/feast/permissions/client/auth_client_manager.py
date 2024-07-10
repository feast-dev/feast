from abc import ABC, abstractmethod

from feast.permissions.auth_model import (
    AuthConfig,
    KubernetesAuthConfig,
    OidcAuthConfig,
)


class AuthenticationClientManager(ABC):
    @abstractmethod
    def get_token(self) -> str:
        """Retrieves the token based on the authentication type configuration"""
        pass


def get_auth_client_manager(auth_config: AuthConfig) -> AuthenticationClientManager:
    if auth_config.type == "oidc":
        assert isinstance(auth_config, OidcAuthConfig)

        from feast.permissions.client.oidc_authentication_client_manager import (
            OidcAuthClientManager,
        )

        return OidcAuthClientManager(auth_config)
    elif auth_config.type == "kubernetes":
        assert isinstance(auth_config, KubernetesAuthConfig)

        from feast.permissions.client.kubernetes_auth_client_manager import (
            KubernetesAuthClientManager,
        )

        return KubernetesAuthClientManager(auth_config)
    else:
        raise RuntimeError(
            f"No Auth client manager implemented for the auth type:${auth_config.type}"
        )
