import logging
import os
from typing import Optional
from unittest.mock import Mock

import jwt
from fastapi import Request
from fastapi.security import OAuth2AuthorizationCodeBearer
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
    Server settings are retrieved from the `auth` configurationof the Feature store.
    """

    _auth_config: OidcAuthConfig

    def __init__(self, auth_config: OidcAuthConfig):
        self._auth_config = auth_config
        self.oidc_discovery_service = OIDCDiscoveryService(
            self._auth_config.auth_discovery_url
        )

    async def _validate_token(self, access_token: str):
        """
        Validate the token extracted from the header of the user request against the OAuth2 server.
        """
        # FastAPI's OAuth2AuthorizationCodeBearer requires a Request type but actually uses only the headers field
        # https://github.com/tiangolo/fastapi/blob/eca465f4c96acc5f6a22e92fd2211675ca8a20c8/fastapi/security/oauth2.py#L380
        request = Mock(spec=Request)
        request.headers = {"Authorization": f"Bearer {access_token}"}

        oauth_2_scheme = OAuth2AuthorizationCodeBearer(
            tokenUrl=self.oidc_discovery_service.get_token_url(),
            authorizationUrl=self.oidc_discovery_service.get_authorization_url(),
            refreshUrl=self.oidc_discovery_service.get_refresh_url(),
        )

        await oauth_2_scheme(request=request)

    @staticmethod
    def _extract_username_or_raise_error(data: dict) -> str:
        """Extract the username from the decoded JWT. Raises if missing — identity is mandatory."""
        if "preferred_username" not in data:
            raise AuthenticationError(
                "Missing preferred_username field in access token."
            )
        return data["preferred_username"]

    def _extract_roles(self, data: dict) -> list[str]:
        """Extract client-scoped roles from `resource_access.<client_id>.roles`."""
        if "resource_access" not in data:
            logger.warning("Missing resource_access field in access token.")
            return []
        client_id = self._auth_config.client_id
        if client_id not in data["resource_access"]:
            logger.warning(
                f"Missing resource_access.{client_id} field in access token. Defaulting to empty roles."
            )
            return []
        return data["resource_access"][client_id]["roles"]

    @staticmethod
    def _extract_claim(data: dict, claim: str) -> list[str]:
        """Extract an optional list-of-strings claim. Returns [] with a warning if missing."""
        if claim not in data:
            logger.warning(
                f"Missing {claim} field in access token. Defaulting to empty {claim}."
            )
            return []
        return data[claim]

    def _decode_token(self, access_token: str) -> dict:
        """Fetch the JWKS signing key and decode + verify the JWT."""
        optional_custom_headers = {"User-agent": "custom-user-agent"}
        jwks_client = PyJWKClient(
            self.oidc_discovery_service.get_jwks_url(), headers=optional_custom_headers
        )
        signing_key = jwks_client.get_signing_key_from_jwt(access_token)
        return jwt.decode(
            access_token,
            signing_key.key,
            algorithms=["RS256"],
            audience="account",
            options={
                "verify_aud": False,
                "verify_signature": True,
                "verify_exp": True,
            },
            leeway=10,  # accepts tokens generated up to 10 seconds in the past, in case of clock skew
        )

    async def user_details_from_access_token(self, access_token: str) -> User:
        """
        Validate the access token then decode it to extract the user credentials,
        roles, groups, and namespaces.

        Returns:
            User: Current user, with associated roles, groups, and namespaces.

        Raises:
            AuthenticationError if any error happens.
        """

        # check if intra server communication
        user = self._get_intra_comm_user(access_token)
        if user:
            return user

        try:
            await self._validate_token(access_token)
            logger.debug("Token successfully validated.")
        except Exception as e:
            logger.error(f"Token validation failed: {e}")
            raise AuthenticationError(f"Invalid token: {e}")

        try:
            data = self._decode_token(access_token)

            current_user = self._extract_username_or_raise_error(data)
            roles = self._extract_roles(data)
            groups = self._extract_claim(data, "groups")
            namespaces = self._extract_claim(data, "namespaces")

            logger.info(
                f"Extracted user {current_user} with roles {roles}, groups {groups}, namespaces {namespaces}"
            )
            return User(
                username=current_user,
                roles=roles,
                groups=groups,
                namespaces=namespaces,
            )
        except jwt.exceptions.InvalidTokenError:
            logger.exception("Exception while parsing the token:")
            raise AuthenticationError("Invalid token.")

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
