import logging
import os

from feast.permissions.auth_model import KubernetesAuthConfig
from feast.permissions.client.auth_client_manager import AuthenticationClientManager

logger = logging.getLogger(__name__)


class KubernetesAuthClientManager(AuthenticationClientManager):
    def __init__(self, auth_config: KubernetesAuthConfig):
        self.auth_config = auth_config
        self.token_file_path = "/var/run/secrets/kubernetes.io/serviceaccount/token"

    def get_token(self):
        try:
            token = self._read_token_from_file()
            return token
        except Exception as e:
            logger.info(f"Error reading token from file: {e}")
            logger.info("Attempting to read token from environment variable.")
            try:
                token = self._read_token_from_env()
                return token
            except Exception as env_e:
                logger.exception(
                    f"Error reading token from environment variable: {env_e}"
                )
                raise env_e

    def _read_token_from_file(self):
        try:
            with open(self.token_file_path, "r") as file:
                token = file.read().strip()
            return token
        except Exception as e:
            raise e

    def _read_token_from_env(self):
        token = os.getenv("LOCAL_K8S_TOKEN")
        if not token:
            raise KeyError("LOCAL_K8S_TOKEN environment variable is not set.")
        return token
