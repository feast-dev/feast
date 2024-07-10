from feast.permissions.auth_model import KubernetesAuthConfig
from feast.permissions.client.auth_client_manager import AuthenticationClientManager


class KubernetesAuthClientManager(AuthenticationClientManager):
    def __init__(self, auth_config: KubernetesAuthConfig):
        self.auth_config = auth_config

    # TODO: needs to implement this for k8 auth.
    def get_token(self):
        return ""
