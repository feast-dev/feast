# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import threading
import time
from typing import Dict, List, Optional

import requests
from requests.auth import AuthBase

from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_rest_client import (
    CatalogAuthError,
    IcebergRestClient,
)

logger = logging.getLogger(__name__)

DEFAULT_OAUTH_SCOPE = "PRINCIPAL_ROLE:ALL"
_DEFAULT_TOKEN_TTL_SECONDS = 3600


class OAuthClientCredentialsTokenProvider:
    """Fetches and caches OAuth2 client-credentials access tokens.

    The token is re-fetched shortly before it expires so that long-running
    jobs (materialization, feature serving) keep working.
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: str = DEFAULT_OAUTH_SCOPE,
        timeout: int = 30,
        refresh_skew_seconds: int = 60,
    ):
        self.token_url = token_url
        self._client_id = client_id
        self._client_secret = client_secret
        self.scope = scope
        self.timeout = timeout
        self.refresh_skew_seconds = refresh_skew_seconds
        self._token: Optional[str] = None
        self._expires_at: float = 0.0
        self._lock = threading.Lock()

    def get_token(self, force_refresh: bool = False) -> str:
        with self._lock:
            now = time.monotonic()
            if (
                force_refresh
                or self._token is None
                or now >= self._expires_at - self.refresh_skew_seconds
            ):
                self._token, ttl = self._fetch_token()
                self._expires_at = now + ttl
            return self._token

    def _fetch_token(self) -> "tuple[str, float]":
        try:
            response = requests.post(
                self.token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._client_id,
                    "client_secret": self._client_secret,
                    "scope": self.scope,
                },
                headers={"Accept": "application/json"},
                timeout=self.timeout,
            )
        except requests.exceptions.RequestException as e:
            raise CatalogAuthError(
                f"Failed to reach OAuth token endpoint {self.token_url}: {e}"
            ) from e

        if not response.ok:
            raise CatalogAuthError(
                f"OAuth token request to {self.token_url} failed: "
                f"HTTP {response.status_code}",
                status_code=response.status_code,
            )
        body = response.json()
        token = body.get("access_token")
        if not token:
            raise CatalogAuthError(
                f"OAuth token response from {self.token_url} has no access_token"
            )
        return token, float(body.get("expires_in", _DEFAULT_TOKEN_TTL_SECONDS))


class _BearerTokenAuth(AuthBase):
    """requests auth hook that attaches a fresh bearer token to every request."""

    def __init__(self, provider: OAuthClientCredentialsTokenProvider):
        self._provider = provider

    def __call__(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        request.headers["Authorization"] = f"Bearer {self._provider.get_token()}"
        return request


class PolarisClient(IcebergRestClient):
    """Apache Polaris client.

    Extends :class:`IcebergRestClient` with:

    - OAuth2 client-credentials authentication with automatic token refresh
      (``client_id`` / ``client_secret``), as an alternative to a static token.
    - Table property management through the standard Iceberg REST
      ``updateTable`` operation (``set-properties`` / ``remove-properties``),
      used to record Feast metadata on the Polaris table.

    Polaris serves the Iceberg REST API under ``<host>/api/catalog`` and uses
    the Polaris catalog name as the ``/v1/{prefix}`` path segment.
    """

    def __init__(
        self,
        endpoint: str,
        warehouse: str,
        token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        scope: str = DEFAULT_OAUTH_SCOPE,
        oauth_server_uri: Optional[str] = None,
        credential_vending: bool = True,
        timeout: int = 30,
    ):
        super().__init__(
            endpoint=endpoint,
            token=None if (client_id and client_secret) else token,
            warehouse=warehouse,
            credential_vending=credential_vending,
            timeout=timeout,
            url_style="prefix",
        )
        self.token_provider: Optional[OAuthClientCredentialsTokenProvider] = None
        if client_id and client_secret:
            self.token_provider = OAuthClientCredentialsTokenProvider(
                token_url=oauth_server_uri or f"{self.endpoint}/v1/oauth/tokens",
                client_id=client_id,
                client_secret=client_secret,
                scope=scope,
                timeout=timeout,
            )
            self._session.auth = _BearerTokenAuth(self.token_provider)

    def set_table_properties(
        self, namespace: str, table: str, properties: Dict[str, str]
    ) -> bool:
        """Set properties on a table. Returns True on success.

        Failures are logged and reported as ``False`` so that metadata sync
        never aborts ``feast apply``.
        """
        if not properties:
            return True
        return self._commit_updates(
            namespace,
            table,
            [
                {
                    "action": "set-properties",
                    "updates": {k: str(v) for k, v in properties.items()},
                }
            ],
        )

    def remove_table_properties(
        self, namespace: str, table: str, keys: List[str]
    ) -> bool:
        """Remove properties from a table. Returns True on success."""
        if not keys:
            return True
        return self._commit_updates(
            namespace, table, [{"action": "remove-properties", "removals": keys}]
        )

    def _commit_updates(
        self, namespace: str, table: str, updates: List[Dict[str, object]]
    ) -> bool:
        url = f"{self._base_url}/namespaces/{namespace}/tables/{table}"
        try:
            response = self._session.post(
                url,
                json={"requirements": [], "updates": updates},
                timeout=self.timeout,
            )
        except (requests.exceptions.RequestException, CatalogAuthError) as e:
            logger.warning("Failed to update table %s.%s: %s", namespace, table, e)
            return False

        if not response.ok:
            logger.warning(
                "Failed to update table %s.%s: HTTP %s %s",
                namespace,
                table,
                response.status_code,
                response.text[:200],
            )
            return False
        return True
