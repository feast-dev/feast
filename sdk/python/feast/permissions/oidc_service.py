import requests


class OIDCDiscoveryService:
    def __init__(self, discovery_url: str):
        self.discovery_url = discovery_url
        self._discovery_data = None  # Initialize it lazily.

    @property
    def discovery_data(self):
        """Lazily fetches and caches the OIDC discovery data."""
        if self._discovery_data is None:
            self._discovery_data = self._fetch_discovery_data()
        return self._discovery_data

    def _fetch_discovery_data(self) -> dict:
        try:
            response = requests.get(self.discovery_url)
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
