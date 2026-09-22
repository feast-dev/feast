import logging
import os
from typing import Any, Optional

import jwt
from jwt import PyJWKClient
from starlette.authentication import (
    AuthenticationError,
)

from feast.permissions.auth.token_parser import TokenParser
from feast.permissions.auth_model import OidcAuthConfig
from feast.permissions.oidc_service import OIDCDiscoveryService
from feast.permissions.user import User

logger = logging.getLogger(__name__)


class OidcTokenParser(TokenParser):
    """
    A `TokenParser` to use an OIDC server to retrieve the user details.
    Server settings are retrieved from the `auth` configuration of the Feature store.

    Token validation is synchronous (JWKS + ``jwt.decode``) so it is safe to call from
    gRPC ThreadPoolExecutor workers. Signature and ``exp`` are always verified;
    ``iss`` and ``aud`` are verified when configured on ``OidcAuthConfig``.
    """

    _auth_config: OidcAuthConfig

    def __init__(self, auth_config: OidcAuthConfig):
        self._auth_config = auth_config
        self.oidc_discovery_service = OIDCDiscoveryService(
            self._auth_config.auth_discovery_url
        )

    async def user_details_from_access_token(self, access_token: str) -> User:
        """
        Validate the access token then decode it to extract the user credential and roles.

        Returns:
            User: Current user, with associated roles.

        Raises:
            AuthenticationError if any error happens.
        """
        return self.user_details_from_access_token_sync(access_token)

    def user_details_from_access_token_sync(self, access_token: str) -> User:
        """
        Synchronous JWT validation and claim extraction.
        """
        user = self._get_intra_comm_user(access_token)
        if user:
            return user

        try:
            data = self._decode_token(access_token)
        except AuthenticationError:
            raise
        except Exception as e:
            logger.error(f"Token validation failed: {e}")
            raise AuthenticationError(f"Invalid token: {e}")

        current_user = self._username_from_claims(data)
        roles = self._roles_from_claims(data)
        logger.info(f"Extracted user {current_user} and roles {roles}")
        return User(username=current_user, roles=roles)

    def _decode_token(self, access_token: str) -> dict[str, Any]:
        """
        Verify signature, exp, and optional iss/aud against the OIDC JWKS.
        """
        optional_custom_headers = {"User-agent": "custom-user-agent"}
        jwks_client = PyJWKClient(
            self.oidc_discovery_service.get_jwks_url(), headers=optional_custom_headers
        )

        try:
            signing_key = jwks_client.get_signing_key_from_jwt(access_token)
            issuer = self._auth_config.issuer
            audience = self._auth_config.audience
            decode_kwargs: dict[str, Any] = {
                "algorithms": ["RS256"],
                "options": {
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_aud": bool(audience),
                    "verify_iss": bool(issuer),
                },
                "leeway": 10,  # accepts tokens generated up to 10 seconds in the past
            }
            if audience:
                decode_kwargs["audience"] = audience
            if issuer:
                decode_kwargs["issuer"] = issuer
            return jwt.decode(access_token, signing_key.key, **decode_kwargs)
        except jwt.exceptions.InvalidTokenError:
            logger.exception("Exception while parsing the token:")
            raise AuthenticationError("Invalid token.")

    def _username_from_claims(self, data: dict[str, Any]) -> str:
        current_user = (
            data.get("preferred_username") or data.get("sub") or data.get("cid")
        )
        if not current_user:
            raise AuthenticationError(
                "Missing preferred_username, sub, and cid in access token."
            )
        return str(current_user)

    def _roles_from_claims(self, data: dict[str, Any]) -> list:
        resource_access = data.get("resource_access")
        if not isinstance(resource_access, dict):
            logger.warning("Missing resource_access field in access token.")
            return []

        client_id = self._auth_config.client_id
        client_access = resource_access.get(client_id)
        if not isinstance(client_access, dict):
            logger.warning(
                f"Missing resource_access.{client_id} field in access token. Defaulting to empty roles."
            )
            return []

        roles = client_access.get("roles")
        if not isinstance(roles, list):
            return []
        return roles

    def _get_intra_comm_user(self, access_token: str) -> Optional[User]:
        intra_communication_base64 = os.getenv("INTRA_COMMUNICATION_BASE64")

        if intra_communication_base64:
            decoded_token = jwt.decode(
                access_token, options={"verify_signature": False}
            )
            if "preferred_username" in decoded_token:
                preferred_username: str = decoded_token["preferred_username"]
                if (
                    preferred_username is not None
                    and preferred_username == intra_communication_base64
                ):
                    return User(username=preferred_username, roles=[])

        return None
