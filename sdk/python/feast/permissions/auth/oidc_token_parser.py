import logging
import os
import ssl
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
    A ``TokenParser`` to use an OIDC server to retrieve the user details.
    Server settings are retrieved from the ``auth`` configuration of the Feature store.

    Incoming tokens that contain a ``kubernetes.io`` claim (i.e. Kubernetes
    service-account tokens) are handled via a lightweight TokenReview that
    extracts only the namespace — no RBAC queries needed.  All other tokens
    follow the standard OIDC/Keycloak JWKS validation path.
    """

    _auth_config: OidcAuthConfig

    def __init__(self, auth_config: OidcAuthConfig):
        self._auth_config = auth_config
        self.oidc_discovery_service = OIDCDiscoveryService(
            self._auth_config.auth_discovery_url,
            verify_ssl=self._auth_config.verify_ssl,
            ca_cert_path=self._auth_config.ca_cert_path,
        )
        self._k8s_auth_api = None

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
        """Extract the username from the decoded JWT. Raises if missing — identity is mandatory.

        Checks ``preferred_username`` first (Keycloak default), then falls back
        to ``upn`` (Azure AD / Entra ID).
        """
        if "preferred_username" in data:
            return data["preferred_username"]
        if "upn" in data:
            return data["upn"]
        raise AuthenticationError(
            "Missing preferred_username or upn field in access token."
        )

    @staticmethod
    def _extract_claim(data: dict, *keys: str, expected_type: type = list):
        """Walk *keys* into *data* and return the leaf value, or ``expected_type()`` if any key is missing or the wrong type."""
        node = data
        path = ".".join(keys)
        for key in keys:
            if not isinstance(node, dict) or key not in node:
                logger.debug(
                    f"Missing {key} in access token claim path '{path}'. Defaulting to {expected_type()}."
                )
                return expected_type()
            node = node[key]
        if not isinstance(node, expected_type):
            logger.debug(
                f"Expected {expected_type.__name__} at '{path}', got {type(node).__name__}. Defaulting to {expected_type()}."
            )
            return expected_type()
        return node

    @staticmethod
    def _is_ssl_error(exc: BaseException) -> bool:
        """Walk the exception chain looking for SSL-related errors."""
        current: Optional[BaseException] = exc
        while current is not None:
            if isinstance(current, ssl.SSLError):
                return True
            current = current.__cause__ or current.__context__
        return False

    def _decode_token(self, access_token: str) -> dict:
        """Fetch the JWKS signing key and decode + verify the JWT."""
        optional_custom_headers = {"User-agent": "custom-user-agent"}
        ssl_ctx = ssl.create_default_context()
        if not self._auth_config.verify_ssl:
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
        elif self._auth_config.ca_cert_path and os.path.exists(
            self._auth_config.ca_cert_path
        ):
            ssl_ctx.load_verify_locations(self._auth_config.ca_cert_path)
        jwks_client = PyJWKClient(
            self.oidc_discovery_service.get_jwks_url(),
            headers=optional_custom_headers,
            ssl_context=ssl_ctx,
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
        roles, and groups.

        A single unverified decode is performed upfront for lightweight routing:
        intra-server communication, Kubernetes SA tokens (identified by the
        ``kubernetes.io`` claim), or standard OIDC/Keycloak JWKS validation.

        Returns:
            User: Current user, with associated roles, groups, or namespaces.

        Raises:
            AuthenticationError if any error happens.
        """
        try:
            unverified = jwt.decode(access_token, options={"verify_signature": False})
        except jwt.exceptions.DecodeError as e:
            raise AuthenticationError(f"Failed to decode token: {e}")

        user = self._get_intra_comm_user(unverified)
        if user:
            return user

        if isinstance(unverified.get("kubernetes.io"), dict):
            logger.debug("Detected kubernetes.io claim — validating via TokenReview")
            try:
                return await self._validate_k8s_sa_token_and_extract_namespace(
                    access_token
                )
            except AuthenticationError:
                raise
            except Exception as e:
                logger.error(f"Kubernetes token validation failed: {e}")
                raise AuthenticationError(f"Kubernetes token validation failed: {e}")

        # Standard OIDC / Keycloak flow
        try:
            await self._validate_token(access_token)
            logger.debug("Token successfully validated.")
        except Exception as e:
            if self._is_ssl_error(e):
                logger.error(
                    "OIDC provider SSL certificate verification failed. "
                    "If using a self-signed certificate, set verify_ssl: false "
                    "or provide a CA certificate via ca_cert_path."
                )
            logger.error(f"Token validation failed: {e}")
            raise AuthenticationError(f"Invalid token: {e}")

        try:
            data = self._decode_token(access_token)

            current_user = self._extract_username_or_raise_error(data)
            roles = (
                self._extract_claim(
                    data, "resource_access", self._auth_config.client_id, "roles"
                )
                if self._auth_config.client_id
                else []
            )
            groups = self._extract_claim(data, "groups")

            logger.info(
                f"Extracted user {current_user} with roles {roles}, groups {groups}"
            )
            return User(
                username=current_user,
                roles=roles,
                groups=groups,
            )
        except jwt.exceptions.PyJWTError as e:
            if self._is_ssl_error(e):
                logger.error(
                    "OIDC JWKS endpoint SSL certificate verification failed. "
                    "If using a self-signed certificate, set verify_ssl: false "
                    "or provide a CA certificate via ca_cert_path."
                )
            logger.exception("Exception while parsing the token:")
            raise AuthenticationError("Invalid token.")

    async def _validate_k8s_sa_token_and_extract_namespace(
        self, access_token: str
    ) -> User:
        """Validate a K8s SA token via TokenReview and extract the namespace.

        Lightweight alternative to full KubernetesTokenParser — only validates
        the token and extracts the namespace from the authenticated identity.
        No RBAC queries (RoleBindings, ClusterRoleBindings) are performed,
        so the server SA needs only ``tokenreviews/create`` permission.
        """
        from kubernetes import client, config

        if self._k8s_auth_api is None:
            config.load_incluster_config()
            self._k8s_auth_api = client.AuthenticationV1Api()

        token_review = client.V1TokenReview(
            spec=client.V1TokenReviewSpec(token=access_token)
        )
        auth_api: client.AuthenticationV1Api = self._k8s_auth_api
        response = auth_api.create_token_review(token_review)

        if not response.status.authenticated:
            raise AuthenticationError(
                f"Kubernetes token validation failed: {response.status.error}"
            )

        username = getattr(response.status.user, "username", "") or ""
        namespaces = []
        if username.startswith("system:serviceaccount:") and username.count(":") >= 3:
            namespaces.append(username.split(":")[2])

        logger.info(f"SA token validated — user: {username}, namespaces: {namespaces}")
        return User(username=username, roles=[], groups=[], namespaces=namespaces)

    @staticmethod
    def _get_intra_comm_user(decoded_token: dict) -> Optional[User]:
        intra_communication_base64 = os.getenv("INTRA_COMMUNICATION_BASE64")

        if intra_communication_base64:
            if "preferred_username" in decoded_token:
                preferred_username: str = decoded_token["preferred_username"]
                if (
                    preferred_username is not None
                    and preferred_username == intra_communication_base64
                ):
                    return User(username=preferred_username, roles=[])

        return None
