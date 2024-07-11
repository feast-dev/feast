import logging

from feast.permissions.auth_model import KubernetesAuthConfig
from feast.permissions.client.auth_client_manager import AuthenticationClientManager

logger = logging.getLogger(__name__)


class KubernetesAuthClientManager(AuthenticationClientManager):
    def __init__(self, auth_config: KubernetesAuthConfig):
        self.auth_config = auth_config

    def get_token(self):
        try:
            with open(
                "/var/run/secrets/kubernetes.io/serviceaccount/token", "r"
            ) as file:
                token = file.read().strip()
            return token
        except Exception as e:
            logger.exception(f"Error reading token: {e}")
            raise e
