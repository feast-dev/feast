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


def invalidate_auth_token(auth_config: AuthConfig) -> bool:
    """Drop any cached token for *auth_config*, returning whether one was held.

    Only the OIDC client manager caches, so this is a no-op for the other auth
    types. Callers that can observe an authentication failure should use it: a
    token the IdP revokes mid-life still looks valid to the client until its
    own expiry, and dropping it bounds that to a single rejected request.
    """
    manager = AuthenticationClientManagerFactory(auth_config).get_auth_client_manager()
    invalidate = getattr(manager, "invalidate_token", None)
    return bool(invalidate()) if callable(invalidate) else False
