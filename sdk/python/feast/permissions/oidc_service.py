import os

import requests


class OIDCDiscoveryService:
    def __init__(
        self, discovery_url: str, verify_ssl: bool = True, ca_cert_path: str = ""
    ):
        self.discovery_url = discovery_url
        self._verify_ssl = verify_ssl
        self._ca_cert_path = ca_cert_path
        self._discovery_data = None  # Initialize it lazily.

    @property
    def discovery_data(self):
        """Lazily fetches and caches the OIDC discovery data."""
        if self._discovery_data is None:
            self._discovery_data = self._fetch_discovery_data()
        return self._discovery_data

    def _get_verify(self):
        if not self._verify_ssl:
            return False
        if self._ca_cert_path and os.path.exists(self._ca_cert_path):
            return self._ca_cert_path
        return True

    def _fetch_discovery_data(self) -> dict:
        try:
            response = requests.get(self.discovery_url, verify=self._get_verify())
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            raise RuntimeError(
                f"Error fetching OIDC discovery response, discovery url - {self.discovery_url}, exception - {e} "
            )

    def get_authorization_url(self) -> str:
        """Returns the authorization endpoint URL."""
        return self.discovery_data.get("authorization_endpoint")

    def get_token_url(self) -> str:
        """Returns the token endpoint URL."""
        return self.discovery_data.get("token_endpoint")

    def get_jwks_url(self) -> str:
        """Returns the jwks endpoint URL."""
        return self.discovery_data.get("jwks_uri")

    def get_refresh_url(self) -> str:
        """Returns the refresh token URL (usually same as token URL)."""
        return self.get_token_url()
