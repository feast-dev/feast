import logging

import jwt

from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import AuthConfig
from feast.permissions.client.auth_client_manager import AuthenticationClientManager

logger = logging.getLogger(__name__)


class IntraCommAuthClientManager(AuthenticationClientManager):
    def __init__(self, auth_config: AuthConfig, intra_communication_base64: str):
        self.auth_config = auth_config
        self.intra_communication_base64 = intra_communication_base64
        logger.debug(f"AuthConfig type set to {self.auth_config.type}")

    def get_token(self):
        if self.auth_config.type == AuthType.OIDC.value:
            payload = {
                "preferred_username": f"{self.intra_communication_base64}",  # Subject claim
            }
        elif self.auth_config.type == AuthType.KUBERNETES.value:
            payload = {
                "sub": f":::{self.intra_communication_base64}",  # Subject claim
            }
        else:
            raise RuntimeError(
                f"No Auth client manager implemented for the auth type:{self.auth_config.type}"
            )

        return jwt.encode(payload, "")
