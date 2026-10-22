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

# IdP-issued tokens keyed by the token-request identity, stored with their
# true expiry. The auth interceptors build a fresh manager for every outbound
# RPC, so instance state would not survive between calls; without this
# cache every RPC pays a discovery GET plus a token POST against the IdP.
# The refresh margin is applied on read, not baked into the stored value, so
# configs sharing IdP credentials but setting different margins can share
# tokens while each still honors its own margin.
# Concurrent misses may fetch in parallel (benign: last write wins).
_token_cache: Dict[Tuple, Tuple[str, float]] = {}
_token_cache_lock = threading.Lock()


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
        """Return a cached IdP token, or obtain a fresh one.

        The cache stores each token's true expiry (its ``exp`` claim, falling
        back to the token response's ``expires_in``), and this config's
        ``token_refresh_margin_seconds`` is applied when reading. Keeping the
        margin out of the stored value lets configs that share IdP credentials
        but set different margins share tokens while each honors its own
        margin. A token whose expiry is unknowable is not cached, preserving
        the previous per-call behavior for opaque tokens.
        """
        cache_key = self._cache_key()
        with _token_cache_lock:
            cached = _token_cache.get(cache_key)
        if cached is not None:
            cached_token, cached_expiry = cached
            margin = self.auth_config.token_refresh_margin_seconds
            if time.time() < cached_expiry - margin:
                return cached_token

        access_token, expires_in = self._request_token_from_idp()

        expiry = self._token_expiry(access_token, expires_in)
        if expiry is not None:
            now = time.time()
            with _token_cache_lock:
                # Prune on miss. Entries are keyed by credential identity, so
                # the cache is bounded by the number of distinct configs, but
                # a long-lived process that rotates credentials would otherwise
                # keep every retired identity forever.
                for key in [k for k, (_, exp) in _token_cache.items() if exp <= now]:
                    del _token_cache[key]
                _token_cache[cache_key] = (access_token, expiry)
        return access_token

    def _cache_key(self) -> Tuple:
        """Identity of the token request: same credentials, same token."""
        return (
            self.auth_config.auth_discovery_url,
            self.auth_config.client_id,
            self.auth_config.client_secret,
            self.auth_config.username,
            self.auth_config.password,
        )

    def invalidate_token(self) -> bool:
        """Drop this config's cached token so the next call refetches.

        Returns whether an entry was actually removed.

        Reuse means a token the IdP revokes mid-life keeps being presented
        until its own expiry, where fetching per call self-corrected. Callers
        that can observe an authentication failure should invalidate on it, so
        the staleness costs one rejected request rather than the remaining
        lifetime of the token.
        """
        with _token_cache_lock:
            return _token_cache.pop(self._cache_key(), None) is not None

    @staticmethod
    def _token_expiry(
        access_token: str, expires_in: Optional[float]
    ) -> Optional[float]:
        """Epoch expiry of *access_token*, or ``None`` when it is unknowable.

        Prefers the token's own ``exp`` claim (authoritative); falls back to
        the token endpoint's ``expires_in``.

        The refresh margin is deliberately not subtracted here. Storing one
        caller's deadline would let another config with a wider margin reuse
        the token past its own safety window, since the margin is not part of
        the cache key.
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
        return exp

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
            if not isinstance(expires_in, (int, float)):
                expires_in = None
            return access_token, expires_in
        else:
            raise RuntimeError(
                f"""Failed to obtain oidc access token:url=[{token_endpoint}] {token_response.status_code} - {token_response.text}"""
            )
