import logging
import os

import jwt
import requests

from feast.permissions.auth_model import OidcClientAuthConfig
from feast.permissions.client.auth_client_manager import AuthenticationClientManager
from feast.permissions.oidc_service import OIDCDiscoveryService

logger = logging.getLogger(__name__)


class OidcAuthClientManager(AuthenticationClientManager):
    def __init__(self, auth_config: OidcClientAuthConfig):
        self.auth_config = auth_config

    def get_token(self):
        intra_communication_base64 = os.getenv("INTRA_COMMUNICATION_BASE64")
        # If intra server communication call
        if intra_communication_base64:
            payload = {
                "preferred_username": f"{intra_communication_base64}",  # Subject claim
            }

            return jwt.encode(payload, "")

        # Fetch the token endpoint from the discovery URL
        token_endpoint = OIDCDiscoveryService(
            self.auth_config.auth_discovery_url
        ).get_token_url()

        # 1) pre-issued JWT supplied in config
        if getattr(self.auth_config, "token", None):
            return self.auth_config.token

        # 2) client_credentials
        if self.auth_config.client_secret and not (
            self.auth_config.username and self.auth_config.password
        ):
            token_request_body = {
                "grant_type": "client_credentials",
                "client_id": self.auth_config.client_id,
                "client_secret": self.auth_config.client_secret,
            }
        # 3) ROPG (username + password + client_secret)
        else:
            token_request_body = {
                "grant_type": "password",
                "client_id": self.auth_config.client_id,
                "client_secret": self.auth_config.client_secret,
                "username": self.auth_config.username,
                "password": self.auth_config.password,
            }
        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        token_response = requests.post(
            token_endpoint, data=token_request_body, headers=headers
        )
        if token_response.status_code == 200:
            access_token = token_response.json()["access_token"]
            if not access_token:
                logger.debug(
                    f"access_token is empty for the client_id=${self.auth_config.client_id}"
                )
                raise RuntimeError("access token is empty")
            return access_token
        else:
            raise RuntimeError(
                f"""Failed to obtain oidc access token:url=[{token_endpoint}] {token_response.status_code} - {token_response.text}"""
            )
