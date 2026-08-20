import asyncio
import os
import ssl
import time
from unittest import mock
from unittest.mock import MagicMock, patch

import assertpy
import jwt
import pytest
from pydantic import ValidationError
from starlette.authentication import (
    AuthenticationError,
)

from feast.permissions.auth.kubernetes_token_parser import KubernetesTokenParser
from feast.permissions.auth.oidc_token_parser import OidcTokenParser
from feast.permissions.auth_model import OidcAuthConfig
from feast.permissions.user import User

_CLIENT_ID = "test"


@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_token_validation_success(
    mock_discovery_data,
    mock_jwt,
    mock_signing_key,
    mock_oauth2,
    oidc_config,
    discovery_data,
    signing_key,
):
    mock_signing_key.return_value = signing_key
    mock_discovery_data.return_value = discovery_data

    user_data = {
        "preferred_username": "my-name",
        "resource_access": {_CLIENT_ID: {"roles": ["reader", "writer"]}},
    }
    mock_jwt.return_value = user_data

    access_token = "aaa-bbb-ccc"
    token_parser = OidcTokenParser(auth_config=oidc_config)
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token=access_token)
    )

    assertpy.assert_that(user).is_type_of(User)
    if isinstance(user, User):
        assertpy.assert_that(user.username).is_equal_to("my-name")
        assertpy.assert_that(sorted(user.roles)).is_equal_to(
            sorted(["reader", "writer"])
        )
        assertpy.assert_that(user.has_matching_role(["reader"])).is_true()
        assertpy.assert_that(user.has_matching_role(["writer"])).is_true()
        assertpy.assert_that(user.has_matching_role(["updater"])).is_false()
        assertpy.assert_that(user.groups).is_equal_to([])
        assertpy.assert_that(user.namespaces).is_equal_to([])


@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_token_missing_roles_key_returns_empty(
    mock_discovery_data,
    mock_jwt,
    mock_signing_key,
    mock_oauth2,
    oidc_config,
    discovery_data,
    signing_key,
):
    mock_signing_key.return_value = signing_key
    mock_discovery_data.return_value = discovery_data

    user_data = {
        "preferred_username": "my-name",
        "resource_access": {_CLIENT_ID: {}},
    }
    mock_jwt.return_value = user_data

    access_token = "aaa-bbb-ccc"
    token_parser = OidcTokenParser(auth_config=oidc_config)
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token=access_token)
    )

    assertpy.assert_that(user).is_type_of(User)
    if isinstance(user, User):
        assertpy.assert_that(user.username).is_equal_to("my-name")
        assertpy.assert_that(user.roles).is_equal_to([])


@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_token_extracts_groups(
    mock_discovery_data,
    mock_jwt,
    mock_signing_key,
    mock_oauth2,
    oidc_config,
    discovery_data,
    signing_key,
):
    mock_signing_key.return_value = signing_key
    mock_discovery_data.return_value = discovery_data

    user_data = {
        "preferred_username": "my-name",
        "resource_access": {_CLIENT_ID: {"roles": ["reader"]}},
        "groups": ["banking-admin", "data-engineers"],
    }
    mock_jwt.return_value = user_data

    access_token = "aaa-bbb-ccc"
    token_parser = OidcTokenParser(auth_config=oidc_config)
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token=access_token)
    )

    assertpy.assert_that(user).is_type_of(User)
    if isinstance(user, User):
        assertpy.assert_that(user.groups).is_equal_to(
            ["banking-admin", "data-engineers"]
        )
        assertpy.assert_that(user.has_matching_group(["banking-admin"])).is_true()
        assertpy.assert_that(user.has_matching_group(["unknown-group"])).is_false()
        assertpy.assert_that(user.namespaces).is_equal_to([])


@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_token_extracts_groups_and_roles(
    mock_discovery_data,
    mock_jwt,
    mock_signing_key,
    mock_oauth2,
    oidc_config,
    discovery_data,
    signing_key,
):
    mock_signing_key.return_value = signing_key
    mock_discovery_data.return_value = discovery_data

    user_data = {
        "preferred_username": "my-name",
        "resource_access": {_CLIENT_ID: {"roles": ["reader", "writer"]}},
        "groups": ["banking-admin", "data-engineers"],
    }
    mock_jwt.return_value = user_data

    access_token = "aaa-bbb-ccc"
    token_parser = OidcTokenParser(auth_config=oidc_config)
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token=access_token)
    )

    assertpy.assert_that(user).is_type_of(User)
    if isinstance(user, User):
        assertpy.assert_that(user.username).is_equal_to("my-name")
        assertpy.assert_that(sorted(user.roles)).is_equal_to(
            sorted(["reader", "writer"])
        )
        assertpy.assert_that(user.groups).is_equal_to(
            ["banking-admin", "data-engineers"]
        )
        assertpy.assert_that(user.has_matching_role(["reader"])).is_true()
        assertpy.assert_that(user.has_matching_group(["banking-admin"])).is_true()


@pytest.mark.parametrize(
    "token_claims, expected_username",
    [
        # Human identity claims keep precedence over the application claims.
        (
            {
                "preferred_username": "my-name",
                "upn": "my-name@example.com",
                "azp": "my-client-app",
            },
            "my-name",
        ),
        (
            {"upn": "my-name@example.com", "azp": "my-client-app"},
            "my-name@example.com",
        ),
        # A human claim still wins when accompanied only by application claims.
        (
            {
                "preferred_username": "my-name",
                "appid": "my-v1-client-app",
                "sub": "my-subject",
            },
            "my-name",
        ),
        ({"upn": "my-name@example.com", "sub": "my-subject"}, "my-name@example.com"),
        # Entra ID client-credentials (app-only) tokens carry no human claim.
        (
            {"azp": "my-client-app", "appid": "my-v1-client-app", "sub": "my-subject"},
            "my-client-app",
        ),
        ({"appid": "my-v1-client-app", "sub": "my-subject"}, "my-v1-client-app"),
        ({"sub": "my-subject"}, "my-subject"),
    ],
)
@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_token_username_claim_precedence(
    mock_discovery_data,
    mock_jwt,
    mock_signing_key,
    mock_oauth2,
    token_claims,
    expected_username,
    oidc_config,
    discovery_data,
    signing_key,
):
    mock_signing_key.return_value = signing_key
    mock_discovery_data.return_value = discovery_data

    mock_jwt.return_value = token_claims

    access_token = "aaa-bbb-ccc"
    token_parser = OidcTokenParser(auth_config=oidc_config)
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token=access_token)
    )

    assertpy.assert_that(user).is_type_of(User)
    if isinstance(user, User):
        assertpy.assert_that(user.username).is_equal_to(expected_username)


@pytest.mark.parametrize(
    "token_claims",
    [
        # No identity claim at all.
        {"resource_access": {_CLIENT_ID: {"roles": ["reader"]}}},
        # Identity claims present but null are skipped, not accepted.
        {"preferred_username": None, "upn": None, "azp": None, "sub": None},
        # A non-string identity claim is skipped, not accepted.
        {"sub": 12345},
    ],
)
@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_token_without_usable_username_claim_fails(
    mock_discovery_data,
    mock_jwt,
    mock_signing_key,
    mock_oauth2,
    token_claims,
    oidc_config,
    discovery_data,
    signing_key,
):
    mock_signing_key.return_value = signing_key
    mock_discovery_data.return_value = discovery_data

    mock_jwt.return_value = token_claims

    access_token = "aaa-bbb-ccc"
    token_parser = OidcTokenParser(auth_config=oidc_config)
    with pytest.raises(AuthenticationError):
        asyncio.run(
            token_parser.user_details_from_access_token(access_token=access_token)
        )


@pytest.mark.parametrize(
    "token_claims, expected_roles",
    [
        # Entra ID app roles arrive in the top-level `roles` claim.
        ({"roles": ["reader", "writer"]}, ["reader", "writer"]),
        # Duplicates within the top-level claim are collapsed.
        ({"roles": ["reader", "reader", "writer"]}, ["reader", "writer"]),
        # Keycloak's nested claim keeps working on its own.
        ({"resource_access": {_CLIENT_ID: {"roles": ["reader"]}}}, ["reader"]),
        # Both shapes present: merged, in order, without duplicates.
        (
            {
                "resource_access": {_CLIENT_ID: {"roles": ["reader", "writer"]}},
                "roles": ["writer", "admin"],
            },
            ["reader", "writer", "admin"],
        ),
    ],
)
@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_token_merges_top_level_and_resource_access_roles(
    mock_discovery_data,
    mock_jwt,
    mock_signing_key,
    mock_oauth2,
    token_claims,
    expected_roles,
    oidc_config,
    discovery_data,
    signing_key,
):
    mock_signing_key.return_value = signing_key
    mock_discovery_data.return_value = discovery_data

    mock_jwt.return_value = {"preferred_username": "my-name", **token_claims}

    access_token = "aaa-bbb-ccc"
    token_parser = OidcTokenParser(auth_config=oidc_config)
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token=access_token)
    )

    assertpy.assert_that(user).is_type_of(User)
    if isinstance(user, User):
        assertpy.assert_that(user.roles).is_equal_to(expected_roles)


@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_identity_and_roles_come_from_verified_decode(
    mock_discovery_data,
    mock_jwt,
    mock_signing_key,
    mock_oauth2,
    oidc_config,
    discovery_data,
    signing_key,
):
    """Identity and roles must come from the signature-verified decode, not the
    unverified decode used only for routing."""
    mock_signing_key.return_value = signing_key
    mock_discovery_data.return_value = discovery_data

    def decode(token, key=None, *args, **kwargs):
        if kwargs.get("options", {}).get("verify_signature") is False:
            # Unverified decode, used only for routing; must not drive identity.
            return {
                "preferred_username": "spoofed-user",
                "resource_access": {_CLIENT_ID: {"roles": ["spoofed-role"]}},
            }
        return {
            "preferred_username": "verified-user",
            "resource_access": {_CLIENT_ID: {"roles": ["reader"]}},
        }

    mock_jwt.side_effect = decode

    access_token = "aaa-bbb-ccc"
    token_parser = OidcTokenParser(auth_config=oidc_config)
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token=access_token)
    )

    assertpy.assert_that(user).is_type_of(User)
    if isinstance(user, User):
        assertpy.assert_that(user.username).is_equal_to("verified-user")
        assertpy.assert_that(user.roles).is_equal_to(["reader"])


@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
def test_oidc_token_validation_failure(mock_oauth2, oidc_config):
    mock_oauth2.side_effect = AuthenticationError("wrong token")

    access_token = "aaa-bbb-ccc"
    token_parser = OidcTokenParser(auth_config=oidc_config)
    with pytest.raises(AuthenticationError):
        asyncio.run(
            token_parser.user_details_from_access_token(access_token=access_token)
        )


@mock.patch.dict(os.environ, {"INTRA_COMMUNICATION_BASE64": "test1234"})
@pytest.mark.parametrize(
    "intra_communication_val, is_intra_server",
    [
        ("test1234", True),
        ("my-name", False),
    ],
)
def test_oidc_inter_server_comm(
    intra_communication_val, is_intra_server, oidc_config, monkeypatch
):
    async def mock_oath2(self, request):
        return "OK"

    monkeypatch.setattr(
        "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__",
        mock_oath2,
    )
    signing_key = MagicMock()
    signing_key.key = "a-key"
    monkeypatch.setattr(
        "feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt",
        lambda self, access_token: signing_key,
    )

    user_data = {
        "preferred_username": f"{intra_communication_val}",
    }

    if not is_intra_server:
        user_data["resource_access"] = {_CLIENT_ID: {"roles": ["reader", "writer"]}}

        monkeypatch.setattr(
            "feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data",
            lambda self, *args, **kwargs: {
                "authorization_endpoint": "https://localhost:8080/realms/master/protocol/openid-connect/auth",
                "token_endpoint": "https://localhost:8080/realms/master/protocol/openid-connect/token",
                "jwks_uri": "https://localhost:8080/realms/master/protocol/openid-connect/certs",
            },
        )

    monkeypatch.setattr(
        "feast.permissions.auth.oidc_token_parser.jwt.decode",
        lambda self, *args, **kwargs: user_data,
    )

    access_token = "aaa-bbb-ccc"
    token_parser = OidcTokenParser(auth_config=oidc_config)
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token=access_token)
    )

    if is_intra_server:
        assertpy.assert_that(user).is_not_none()
        assertpy.assert_that(user.username).is_equal_to(intra_communication_val)
        assertpy.assert_that(user.roles).is_equal_to([])
    else:
        assertpy.assert_that(user).is_not_none()
        assertpy.assert_that(user).is_type_of(User)
        if isinstance(user, User):
            assertpy.assert_that(user.username).is_equal_to("my-name")
            assertpy.assert_that(sorted(user.roles)).is_equal_to(
                sorted(["reader", "writer"])
            )
            assertpy.assert_that(user.has_matching_role(["reader"])).is_true()
            assertpy.assert_that(user.has_matching_role(["writer"])).is_true()
            assertpy.assert_that(user.has_matching_role(["updater"])).is_false()


# ---------------------------------------------------------------------------
# JWKS client lifecycle (one lazy client per parser)
# ---------------------------------------------------------------------------


@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_jwks_client_is_lazy_and_reused_per_parser(
    mock_discovery_data,
    mock_jwks_client_cls,
    mock_jwt,
    mock_oauth2,
    oidc_config,
    discovery_data,
):
    """One JWKS client per parser: built on the first request (not at
    construction, which would move a blocking discovery fetch into server
    startup), reused across requests, and scoped to the parser instance."""
    mock_discovery_data.return_value = discovery_data
    mock_jwt.return_value = {"preferred_username": "my-name"}

    token_parser = OidcTokenParser(auth_config=oidc_config)
    assertpy.assert_that(mock_jwks_client_cls.call_count).is_equal_to(0)

    for _ in range(3):
        asyncio.run(
            token_parser.user_details_from_access_token(access_token="aaa-bbb-ccc")
        )
    assertpy.assert_that(mock_jwks_client_cls.call_count).is_equal_to(1)

    # A second parser must not see the first parser's client: each parser
    # verifies against the JWKS of its own configured provider.
    other_parser = OidcTokenParser(auth_config=oidc_config)
    asyncio.run(other_parser.user_details_from_access_token(access_token="aaa-bbb-ccc"))
    assertpy.assert_that(mock_jwks_client_cls.call_count).is_equal_to(2)


@pytest.mark.parametrize("verify_ssl", [True, False])
@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_jwks_client_ssl_context_follows_config(
    mock_discovery_data,
    mock_jwks_client_cls,
    mock_jwt,
    mock_oauth2,
    verify_ssl,
    discovery_data,
):
    """The client is built with the discovery JWKS URL and an SSL context
    matching verify_ssl: default configs must keep certificate verification
    on, and verify_ssl=False must be the only way to turn it off."""
    mock_discovery_data.return_value = discovery_data
    mock_jwt.return_value = {"preferred_username": "my-name"}

    token_parser = OidcTokenParser(auth_config=_oidc_config_with(verify_ssl=verify_ssl))
    asyncio.run(token_parser.user_details_from_access_token(access_token="aaa-bbb-ccc"))

    call = mock_jwks_client_cls.call_args
    assertpy.assert_that(call.args[0]).is_equal_to(discovery_data["jwks_uri"])
    ssl_ctx = call.kwargs["ssl_context"]
    if verify_ssl:
        assertpy.assert_that(ssl_ctx.verify_mode).is_equal_to(ssl.CERT_REQUIRED)
        assertpy.assert_that(ssl_ctx.check_hostname).is_true()
    else:
        assertpy.assert_that(ssl_ctx.verify_mode).is_equal_to(ssl.CERT_NONE)
        assertpy.assert_that(ssl_ctx.check_hostname).is_false()


@pytest.mark.parametrize(
    "overrides,expected_lifespan,expected_timeout",
    [
        ({}, 300, 10),
        (
            {
                "jwks_cache_lifespan_seconds": 60,
                "jwks_request_timeout_seconds": 2.5,
            },
            60,
            2.5,
        ),
    ],
)
@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_jwks_client_cache_and_timeout_follow_config(
    mock_discovery_data,
    mock_jwks_client_cls,
    mock_jwt,
    mock_oauth2,
    overrides,
    expected_lifespan,
    expected_timeout,
    discovery_data,
):
    """The JWK-set cache lifespan and the fetch timeout are operator-tunable:
    the lifespan bounds how long a revoked key keeps validating, and the
    timeout bounds how long an unresponsive IdP blocks the serving path."""
    mock_discovery_data.return_value = discovery_data
    mock_jwt.return_value = {"preferred_username": "my-name"}

    token_parser = OidcTokenParser(auth_config=_oidc_config_with(**overrides))
    asyncio.run(token_parser.user_details_from_access_token(access_token="aaa-bbb-ccc"))

    kwargs = mock_jwks_client_cls.call_args.kwargs
    assertpy.assert_that(kwargs["lifespan"]).is_equal_to(expected_lifespan)
    assertpy.assert_that(kwargs["timeout"]).is_equal_to(expected_timeout)


@pytest.mark.parametrize(
    "overrides",
    [
        {"jwks_cache_lifespan_seconds": 0},
        {"jwks_cache_lifespan_seconds": -1},
        {"jwks_request_timeout_seconds": 0},
        {"jwks_request_timeout_seconds": -1},
    ],
)
def test_oidc_jwks_tunables_reject_non_positive_values(overrides):
    """A non-positive lifespan would expire the cache immediately, silently
    restoring a JWKS fetch per request; a non-positive timeout is equally
    meaningless. Reject both at config load rather than at serving time."""
    with pytest.raises(ValidationError):
        _oidc_config_with(**overrides)


@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_jwks_client_construction_failure_is_retried(
    mock_discovery_data,
    mock_jwks_client_cls,
    mock_jwt,
    mock_oauth2,
    oidc_config,
    discovery_data,
):
    """A failed first construction must leave the parser able to retry on
    the next request: the parser is a process singleton, so caching a failed
    or half-built client would wedge authentication until restart."""
    mock_discovery_data.return_value = discovery_data
    mock_jwt.return_value = {"preferred_username": "my-name"}
    mock_jwks_client_cls.side_effect = [RuntimeError("IdP unreachable"), MagicMock()]

    token_parser = OidcTokenParser(auth_config=oidc_config)
    with pytest.raises(RuntimeError):
        asyncio.run(
            token_parser.user_details_from_access_token(access_token="aaa-bbb-ccc")
        )

    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token="aaa-bbb-ccc")
    )
    assertpy.assert_that(user.username).is_equal_to("my-name")
    assertpy.assert_that(mock_jwks_client_cls.call_count).is_equal_to(2)


# ---------------------------------------------------------------------------
# Optional audience / issuer verification (opt-in via OidcAuthConfig)
# ---------------------------------------------------------------------------


def _oidc_config_with(**overrides) -> OidcAuthConfig:
    return OidcAuthConfig(
        auth_discovery_url="https://localhost:8080/realms/master/.well-known/openid-configuration",
        client_id=_CLIENT_ID,
        type="oidc",
        **overrides,
    )


@pytest.fixture(scope="module")
def rsa_keypair() -> tuple:
    """A real RSA keypair, so the aud/iss tests exercise the real ``jwt.decode``."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    return private_pem, public_pem


def _make_token(private_pem: bytes, claims: dict) -> str:
    now = int(time.time())
    return jwt.encode(
        {"iat": now, "exp": now + 300, **claims}, private_pem, algorithm="RS256"
    )


@pytest.mark.parametrize(
    "audience,issuer",
    [
        (None, None),
        ("api://feast-server", None),
        (None, "https://idp.example.com/realm"),
        ("api://feast-server", "https://idp.example.com/realm"),
    ],
)
@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_decode_verification_options_follow_config(
    mock_discovery_data,
    mock_jwt,
    mock_signing_key,
    mock_oauth2,
    audience,
    issuer,
    discovery_data,
    signing_key,
):
    """The verified decode enables aud/iss checks exactly when the config
    provides expected values, and stays permissive otherwise."""
    mock_signing_key.return_value = signing_key
    mock_discovery_data.return_value = discovery_data
    mock_jwt.return_value = {"preferred_username": "my-name"}

    token_parser = OidcTokenParser(
        auth_config=_oidc_config_with(audience=audience, issuer=issuer)
    )
    asyncio.run(token_parser.user_details_from_access_token(access_token="aaa-bbb-ccc"))

    verified_calls = [
        c
        for c in mock_jwt.call_args_list
        if c.kwargs.get("options", {}).get("verify_signature") is not False
    ]
    assertpy.assert_that(verified_calls).is_length(1)
    kwargs = verified_calls[0].kwargs
    assertpy.assert_that(kwargs["options"]["verify_aud"]).is_equal_to(
        audience is not None
    )
    assertpy.assert_that(kwargs["options"]["verify_iss"]).is_equal_to(
        issuer is not None
    )
    assertpy.assert_that(kwargs["audience"]).is_equal_to(
        audience if audience is not None else "account"
    )
    assertpy.assert_that(kwargs["issuer"]).is_equal_to(issuer)


@pytest.mark.parametrize(
    "config_kwargs,claims,should_authenticate",
    [
        # Opt-in audience: match accepted, mismatch and missing rejected.
        ({"audience": "api://feast-server"}, {"aud": "api://feast-server"}, True),
        ({"audience": "api://feast-server"}, {"aud": "api://another-app"}, False),
        ({"audience": "api://feast-server"}, {}, False),
        # Opt-in issuer: match accepted, mismatch rejected.
        (
            {"issuer": "https://idp.example.com/expected"},
            {"iss": "https://idp.example.com/expected"},
            True,
        ),
        (
            {"issuer": "https://idp.example.com/expected"},
            {"iss": "https://idp.example.com/other"},
            False,
        ),
        # Default config: neither claim is verified, so a token minted for a
        # different resource still authenticates (pre-existing behavior).
        ({}, {"aud": "api://another-app", "iss": "https://idp.example.com/any"}, True),
    ],
)
@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_audience_issuer_verification_end_to_end(
    mock_discovery_data,
    mock_signing_key,
    mock_oauth2,
    config_kwargs,
    claims,
    should_authenticate,
    discovery_data,
    rsa_keypair,
):
    """Real RS256-signed tokens through the real ``jwt.decode``: opt-in checks
    reject mismatched aud/iss and the default stays permissive."""
    private_pem, public_pem = rsa_keypair
    mock_discovery_data.return_value = discovery_data
    key = MagicMock()
    key.key = public_pem
    mock_signing_key.return_value = key

    token = _make_token(private_pem, {"preferred_username": "my-name", **claims})
    token_parser = OidcTokenParser(auth_config=_oidc_config_with(**config_kwargs))

    if should_authenticate:
        user = asyncio.run(
            token_parser.user_details_from_access_token(access_token=token)
        )
        assertpy.assert_that(user).is_type_of(User)
        if isinstance(user, User):
            assertpy.assert_that(user.username).is_equal_to("my-name")
    else:
        with pytest.raises(AuthenticationError):
            asyncio.run(token_parser.user_details_from_access_token(access_token=token))


@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_default_supports_v1_tokens_against_v2_discovery(
    mock_discovery_data,
    mock_signing_key,
    mock_oauth2,
    discovery_data,
    rsa_keypair,
):
    """Pins the Entra ID v1-token-against-v2-discovery setup: with no expected
    audience or issuer configured, a v1.0-shaped app-only token (issuer under
    ``sts.windows.net``, ``api://`` audience, ``appid`` identity) validates
    against a v2.0-style discovery document, because discovery is used only to
    source the JWKS signing keys. A future strict-by-default change would
    break real deployments and must fail here first."""
    private_pem, public_pem = rsa_keypair
    mock_discovery_data.return_value = discovery_data
    key = MagicMock()
    key.key = public_pem
    mock_signing_key.return_value = key

    token = _make_token(
        private_pem,
        {
            "iss": "https://sts.windows.net/11111111-2222-3333-4444-555555555555/",
            "aud": "api://66666666-7777-8888-9999-000000000000",
            "appid": "client-app-id",
            "roles": ["reader"],
        },
    )
    token_parser = OidcTokenParser(auth_config=_oidc_config_with())

    user = asyncio.run(token_parser.user_details_from_access_token(access_token=token))

    assertpy.assert_that(user).is_type_of(User)
    if isinstance(user, User):
        assertpy.assert_that(user.username).is_equal_to("client-app-id")
        assertpy.assert_that(user.roles).is_equal_to(["reader"])


# TODO RBAC: Move role bindings to a reusable fixture
@patch("feast.permissions.auth.kubernetes_token_parser.config.load_incluster_config")
@patch("feast.permissions.auth.kubernetes_token_parser.jwt.decode")
@patch(
    "feast.permissions.auth.kubernetes_token_parser.client.RbacAuthorizationV1Api.list_namespaced_role_binding"
)
def test_k8s_token_validation_success(
    mock_rb,
    mock_jwt,
    mock_config,
    rolebindings,
    monkeypatch,
    my_namespace,
    sa_name,
    sa_namespace,
):
    monkeypatch.setattr(
        "feast.permissions.auth.kubernetes_token_parser.KubernetesTokenParser._read_namespace_from_file",
        lambda self: my_namespace,
    )
    subject = f"system:serviceaccount:{sa_namespace}:{sa_name}"
    mock_jwt.return_value = {"sub": subject}

    mock_rb.return_value = rolebindings["items"]

    roles = rolebindings["roles"]

    access_token = "aaa-bbb-ccc"
    token_parser = KubernetesTokenParser()
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token=access_token)
    )

    assertpy.assert_that(user).is_type_of(User)
    if isinstance(user, User):
        assertpy.assert_that(user.username).is_equal_to(f"{sa_namespace}:{sa_name}")
        assertpy.assert_that(sorted(user.roles)).is_equal_to(sorted(roles))
        for r in roles:
            assertpy.assert_that(user.has_matching_role([r])).is_true()
        assertpy.assert_that(user.has_matching_role(["foo"])).is_false()


@patch("feast.permissions.auth.kubernetes_token_parser.config.load_incluster_config")
@patch("feast.permissions.auth.kubernetes_token_parser.jwt.decode")
def test_k8s_token_validation_failure(mock_jwt, mock_config):
    subject = "wrong-subject"
    mock_jwt.return_value = {"sub": subject}

    access_token = "aaa-bbb-ccc"
    token_parser = KubernetesTokenParser()
    with pytest.raises(AuthenticationError):
        asyncio.run(
            token_parser.user_details_from_access_token(access_token=access_token)
        )


@mock.patch.dict(os.environ, {"INTRA_COMMUNICATION_BASE64": "test1234"})
@pytest.mark.parametrize(
    "intra_communication_val, is_intra_server",
    [
        ("test1234", True),
        ("my-name", False),
    ],
)
def test_k8s_inter_server_comm(
    intra_communication_val,
    is_intra_server,
    oidc_config,
    request,
    rolebindings,
    monkeypatch,
):
    if is_intra_server:
        subject = f":::{intra_communication_val}"
    else:
        sa_name = request.getfixturevalue("sa_name")
        sa_namespace = request.getfixturevalue("sa_namespace")
        my_namespace = request.getfixturevalue("my_namespace")
        subject = f"system:serviceaccount:{sa_namespace}:{sa_name}"
        rolebindings = request.getfixturevalue("rolebindings")

        monkeypatch.setattr(
            "feast.permissions.auth.kubernetes_token_parser.client.RbacAuthorizationV1Api.list_namespaced_role_binding",
            lambda *args, **kwargs: rolebindings["items"],
        )
        monkeypatch.setattr(
            "feast.permissions.client.kubernetes_auth_client_manager.KubernetesAuthClientManager.get_token",
            lambda self: "my-token",
        )
        monkeypatch.setattr(
            "feast.permissions.auth.kubernetes_token_parser.KubernetesTokenParser._read_namespace_from_file",
            lambda self: my_namespace,
        )

    monkeypatch.setattr(
        "feast.permissions.auth.kubernetes_token_parser.config.load_incluster_config",
        lambda: None,
    )

    monkeypatch.setattr(
        "feast.permissions.auth.kubernetes_token_parser.jwt.decode",
        lambda *args, **kwargs: {"sub": subject},
    )

    roles = rolebindings["roles"]

    access_token = "aaa-bbb-ccc"
    token_parser = KubernetesTokenParser()
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token=access_token)
    )

    if is_intra_server:
        assertpy.assert_that(user).is_not_none()
        assertpy.assert_that(user.username).is_equal_to(intra_communication_val)
        assertpy.assert_that(user.roles).is_equal_to([])
    else:
        assertpy.assert_that(user).is_type_of(User)
        if isinstance(user, User):
            assertpy.assert_that(user.username).is_equal_to(f"{sa_namespace}:{sa_name}")
            assertpy.assert_that(sorted(user.roles)).is_equal_to(sorted(roles))
            for r in roles:
                assertpy.assert_that(user.has_matching_role([r])).is_true()
            assertpy.assert_that(user.has_matching_role(["foo"])).is_false()


# ---------------------------------------------------------------------------
#  OidcTokenParser — SA token routing
# ---------------------------------------------------------------------------


@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_parser_handles_sa_token_via_token_review(
    mock_discovery_data, mock_jwt_decode, oidc_config, discovery_data
):
    """When a token contains kubernetes.io claim, _handle_sa_token is called (not the OIDC JWKS path)."""
    mock_discovery_data.return_value = discovery_data

    mock_jwt_decode.return_value = {
        "kubernetes.io": {"namespace": "feast"},
        "sub": "system:serviceaccount:feast:feast",
    }

    sa_user = User(
        username="system:serviceaccount:feast:feast",
        roles=[],
        groups=[],
        namespaces=["feast"],
    )

    token_parser = OidcTokenParser(auth_config=oidc_config)

    with patch.object(
        token_parser,
        "_validate_k8s_sa_token_and_extract_namespace",
        return_value=sa_user,
    ) as mock_handle:
        user = asyncio.run(
            token_parser.user_details_from_access_token(access_token="sa-token")
        )
        mock_handle.assert_called_once_with("sa-token")

    assertpy.assert_that(user.username).is_equal_to("system:serviceaccount:feast:feast")
    assertpy.assert_that(user.namespaces).is_equal_to(["feast"])
    assertpy.assert_that(user.roles).is_equal_to([])
    assertpy.assert_that(user.groups).is_equal_to([])


@patch(
    "feast.permissions.auth.oidc_token_parser.OAuth2AuthorizationCodeBearer.__call__"
)
@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_parser_routes_keycloak_token_normally(
    mock_discovery_data,
    mock_jwt_decode,
    mock_signing_key,
    mock_oauth2,
    oidc_config,
    discovery_data,
    signing_key,
):
    """When a token does NOT contain kubernetes.io claim, it should follow the OIDC path."""
    mock_signing_key.return_value = signing_key
    mock_discovery_data.return_value = discovery_data

    keycloak_payload = {
        "preferred_username": "testuser",
        "resource_access": {_CLIENT_ID: {"roles": ["reader"]}},
        "groups": ["data-team"],
    }
    mock_jwt_decode.return_value = keycloak_payload

    token_parser = OidcTokenParser(auth_config=oidc_config)
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token="keycloak-jwt")
    )

    assertpy.assert_that(user.username).is_equal_to("testuser")
    assertpy.assert_that(user.roles).is_equal_to(["reader"])
    assertpy.assert_that(user.groups).is_equal_to(["data-team"])
