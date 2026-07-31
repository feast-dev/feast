import logging
import os
import threading
import time
from typing import Dict, Optional, Tuple

import jwt
import requests

from feast.permissions.auth_model import OidcClientAuthConfig
from feast.permissions.client.auth_client_manager import AuthenticationClientManager
from feast.permissions.oidc_service import OIDCDiscoveryService

logger = logging.getLogger(__name__)

SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"

# IdP-issued tokens cached until near expiry, keyed by the token-request
# identity. The auth interceptors build a fresh manager for every outbound
# RPC, so instance state would not survive between calls; without this
# cache every RPC pays a discovery GET plus a token POST against the IdP.
# Concurrent misses may fetch in parallel (benign: last write wins).
_token_cache: Dict[Tuple, Tuple[str, float]] = {}
_token_cache_lock = threading.Lock()

# Stop reusing a token this many seconds before its expiry, so a reused
# token cannot expire between header injection and server-side validation.
_TOKEN_REFRESH_MARGIN_SECONDS = 30


class OidcAuthClientManager(AuthenticationClientManager):
    def __init__(self, auth_config: OidcClientAuthConfig):
        self.auth_config = auth_config

    def get_token(self):
        intra_communication_base64 = os.getenv("INTRA_COMMUNICATION_BASE64")
        if intra_communication_base64:
            payload = {
                "preferred_username": f"{intra_communication_base64}",
            }
            return jwt.encode(payload, "", algorithm="none")

        if self.auth_config.token:
            return self.auth_config.token
        elif self.auth_config.token_env_var:
            env_token = os.getenv(self.auth_config.token_env_var)
            if env_token:
                return env_token
            else:
                raise PermissionError(
                    f"token_env_var='{self.auth_config.token_env_var}' is configured "
                    f"but the environment variable is not set or is empty."
                )
        elif self.auth_config.client_secret:
            return self._fetch_token_from_idp()
        else:
            env_token = os.getenv("FEAST_OIDC_TOKEN")
            if env_token:
                return env_token

            sa_token = self._read_sa_token()
            if sa_token:
                return sa_token

            raise PermissionError(
                "No OIDC token source configured. Provide one of: "
                "'token', 'token_env_var', 'client_secret' (with "
                "'auth_discovery_url' and 'client_id'), set the "
                "FEAST_OIDC_TOKEN environment variable, or run inside "
                "a Kubernetes pod with a mounted service account token."
            )

    @staticmethod
    def _read_sa_token() -> Optional[str]:
        """Read the Kubernetes service account token from the standard mount path."""
        if os.path.isfile(SA_TOKEN_PATH):
            with open(SA_TOKEN_PATH) as f:
                token = f.read().strip()
            if token:
                return token
        return None

    def _fetch_token_from_idp(self) -> str:
        """Return a cached IdP token, or obtain and cache a fresh one.

        Tokens are reused until ``_TOKEN_REFRESH_MARGIN_SECONDS`` before
        their expiry (read from the token's ``exp`` claim, falling back to
        the token response's ``expires_in``); a token whose expiry cannot
        be determined is not cached, preserving the previous per-call
        behavior for opaque tokens.
        """
        cache_key = (
            self.auth_config.auth_discovery_url,
            self.auth_config.client_id,
            self.auth_config.client_secret,
            self.auth_config.username,
            self.auth_config.password,
        )
        with _token_cache_lock:
            cached = _token_cache.get(cache_key)
            if cached is not None and time.time() < cached[1]:
                return cached[0]

        access_token, expires_in = self._request_token_from_idp()

        refresh_deadline = self._token_refresh_deadline(access_token, expires_in)
        if refresh_deadline is not None:
            with _token_cache_lock:
                _token_cache[cache_key] = (access_token, refresh_deadline)
        return access_token

    @staticmethod
    def _token_refresh_deadline(
        access_token: str, expires_in: Optional[float]
    ) -> Optional[float]:
        """Epoch time to stop reusing *access_token*, or ``None`` to skip caching.

        Prefers the token's own ``exp`` claim (authoritative); falls back to
        the token endpoint's ``expires_in``. Returns ``None`` when neither is
        usable or the remaining lifetime is inside the refresh margin.
        """
        exp: Optional[float] = None
        try:
            claims = jwt.decode(access_token, options={"verify_signature": False})
            claim = claims.get("exp")
            if isinstance(claim, (int, float)):
                exp = float(claim)
        except jwt.exceptions.DecodeError:
            pass
        if exp is None and isinstance(expires_in, (int, float)):
            exp = time.time() + float(expires_in)
        if exp is None:
            return None
        deadline = exp - _TOKEN_REFRESH_MARGIN_SECONDS
        if deadline <= time.time():
            return None
        return deadline

    def _request_token_from_idp(self) -> Tuple[str, Optional[float]]:
        """Obtain an access token via client_credentials or ROPG flow.

        Returns the token and the token response's ``expires_in`` (seconds),
        when the IdP provides one.
        """
        if self.auth_config.auth_discovery_url is None:
            raise ValueError(
                "auth_discovery_url is required for IDP token fetch "
                "(client_credentials or ROPG flow)."
            )
        discovery = OIDCDiscoveryService(
            self.auth_config.auth_discovery_url,
            verify_ssl=self.auth_config.verify_ssl,
            ca_cert_path=self.auth_config.ca_cert_path,
        )
        token_endpoint = discovery.get_token_url()

        if self.auth_config.client_secret and not (
            self.auth_config.username and self.auth_config.password
        ):
            token_request_body = {
                "grant_type": "client_credentials",
                "client_id": self.auth_config.client_id,
                "client_secret": self.auth_config.client_secret,
            }
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
            token_endpoint,
            data=token_request_body,
            headers=headers,
            verify=discovery._get_verify(),
        )

        if token_response.status_code == 200:
            response_body = token_response.json()
            access_token = response_body["access_token"]
            if not access_token:
                logger.debug(
                    f"access_token is empty for the client_id=${self.auth_config.client_id}"
                )
                raise RuntimeError("access token is empty")
            expires_in = response_body.get("expires_in")
            return access_token, expires_in if isinstance(
                expires_in, (int, float)
            ) else None
        else:
            raise RuntimeError(
                f"""Failed to obtain oidc access token:url=[{token_endpoint}] {token_response.status_code} - {token_response.text}"""
            )
