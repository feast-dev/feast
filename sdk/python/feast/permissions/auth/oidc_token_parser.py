import logging
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
from feast.permissions.user import User

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class OidcTokenParser(TokenParser):
    """
    A `TokenParser` to use an OIDC server to retrieve the user details.
    Server settings are retrieved from the `auth` configurationof the Feature store.
    """

    _auth_config: OidcAuthConfig

    def __init__(self, auth_config: OidcAuthConfig):
        self._auth_config = auth_config

    async def _validate_token(self, access_token: str):
        """
        Validate the token extracted from the headrer of the user request against the OAuth2 server.
        """
        # FastAPI's OAuth2AuthorizationCodeBearer requires a Request type but actually uses only the headers field
        # https://github.com/tiangolo/fastapi/blob/eca465f4c96acc5f6a22e92fd2211675ca8a20c8/fastapi/security/oauth2.py#L380
        request = Mock(spec=Request)
        request.headers = {"Authorization": f"Bearer {access_token}"}

        oauth_2_scheme = OAuth2AuthorizationCodeBearer(
            tokenUrl=f"{self._auth_config.auth_server_url}/realms/{self._auth_config.realm}/protocol/openid-connect/token",
            authorizationUrl=f"{self._auth_config.auth_server_url}/realms/{self._auth_config.realm}/protocol/openid-connect/auth",
            refreshUrl=f"{self._auth_config.auth_server_url}/realms/{self._auth_config.realm}/protocol/openid-connect/token",
        )

        await oauth_2_scheme(request=request)

    async def user_details_from_access_token(self, access_token: str) -> User:
        """
        Validate the access token then decode it to extract the user credential and roles.

        Returns:
            User: Current user, with associated roles.

        Raises:
            AuthenticationError if any error happens.
        """

        try:
            await self._validate_token(access_token)
            logger.info("Validated token")
        except Exception as e:
            raise AuthenticationError(f"Invalid token: {e}")

        url = f"{self._auth_config.auth_server_url}/realms/{self._auth_config.realm}/protocol/openid-connect/certs"
        optional_custom_headers = {"User-agent": "custom-user-agent"}
        jwks_client = PyJWKClient(url, headers=optional_custom_headers)

        try:
            signing_key = jwks_client.get_signing_key_from_jwt(access_token)
            data = jwt.decode(
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

            if "preferred_username" not in data:
                raise AuthenticationError(
                    "Missing preferred_username field in access token."
                )
            current_user = data["preferred_username"]

            if "resource_access" not in data:
                raise AuthenticationError(
                    "Missing resource_access field in access token."
                )
            client_id = self._auth_config.client_id
            if client_id not in data["resource_access"]:
                raise AuthenticationError(
                    f"Missing resource_access.{client_id} field in access token."
                )
            if "roles" not in data["resource_access"][client_id]:
                raise AuthenticationError(
                    f"Missing resource_access.{client_id}.roles field in access token."
                )
            roles = data["resource_access"][client_id]["roles"]
            logger.info(f"Extracted user {current_user} and roles {roles}")
            return User(username=current_user, roles=roles)
        except jwt.exceptions.InvalidTokenError:
            logger.exception("Exception while parsing the token:")
            raise AuthenticationError("Invalid token.")
