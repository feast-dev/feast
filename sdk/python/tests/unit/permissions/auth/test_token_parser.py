import asyncio
import os
from datetime import datetime, timedelta, timezone
from unittest import mock
from unittest.mock import MagicMock, patch

import assertpy
import jwt
import pytest
from starlette.authentication import (
    AuthenticationError,
)

from feast.permissions.auth.kubernetes_token_parser import KubernetesTokenParser
from feast.permissions.auth.oidc_token_parser import OidcTokenParser
from feast.permissions.auth_model import OidcAuthConfig
from feast.permissions.user import User
from tests.unit.permissions.auth.rsa_jwt import (
    AUDIENCE,
    ISSUER,
    encode_jwt,
    generate_rsa_keypair,
    patch_oidc_jwks,
)
from tests.unit.permissions.auth.rsa_jwt import (
    CLIENT_ID as OKTA_CLIENT_ID,
)

_CLIENT_ID = "test"


@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_token_validation_success(
    mock_discovery_data, mock_jwt, mock_signing_key, oidc_config
):
    signing_key = MagicMock()
    signing_key.key = "a-key"
    mock_signing_key.return_value = signing_key

    mock_discovery_data.return_value = {
        "authorization_endpoint": "https://localhost:8080/realms/master/protocol/openid-connect/auth",
        "token_endpoint": "https://localhost:8080/realms/master/protocol/openid-connect/token",
        "jwks_uri": "https://localhost:8080/realms/master/protocol/openid-connect/certs",
    }

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
        assertpy.assert_that(user.roles.sort()).is_equal_to(["reader", "writer"].sort())
        assertpy.assert_that(user.has_matching_role(["reader"])).is_true()
        assertpy.assert_that(user.has_matching_role(["writer"])).is_true()
        assertpy.assert_that(user.has_matching_role(["updater"])).is_false()


@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_token_validation_failure(
    mock_discovery_data, mock_jwt, mock_signing_key, oidc_config
):
    signing_key = MagicMock()
    signing_key.key = "a-key"
    mock_signing_key.return_value = signing_key
    mock_discovery_data.return_value = {
        "authorization_endpoint": "https://localhost:8080/realms/master/protocol/openid-connect/auth",
        "token_endpoint": "https://localhost:8080/realms/master/protocol/openid-connect/token",
        "jwks_uri": "https://localhost:8080/realms/master/protocol/openid-connect/certs",
    }
    mock_jwt.side_effect = jwt.exceptions.InvalidTokenError("wrong token")

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
            assertpy.assert_that(user.roles.sort()).is_equal_to(
                ["reader", "writer"].sort()
            )
            assertpy.assert_that(user.has_matching_role(["reader"])).is_true()
            assertpy.assert_that(user.has_matching_role(["writer"])).is_true()
            assertpy.assert_that(user.has_matching_role(["updater"])).is_false()


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
        assertpy.assert_that(user.roles.sort()).is_equal_to(roles.sort())
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
            assertpy.assert_that(user.roles.sort()).is_equal_to(roles.sort())
            for r in roles:
                assertpy.assert_that(user.has_matching_role([r])).is_true()
            assertpy.assert_that(user.has_matching_role(["foo"])).is_false()


@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
@pytest.mark.parametrize(
    "claims, expected_user, expected_roles",
    [
        ({"sub": "okta-sub"}, "okta-sub", []),
        ({"cid": "okta-cid"}, "okta-cid", []),
        (
            {"preferred_username": "alice", "sub": "s", "cid": "c"},
            "alice",
            [],
        ),
        ({"sub": "s", "cid": "c"}, "s", []),
        (
            {
                "sub": "okta-sub",
                "resource_access": {_CLIENT_ID: {"roles": ["reader"]}},
            },
            "okta-sub",
            ["reader"],
        ),
        (
            {"sub": "okta-sub", "resource_access": {"other": {"roles": ["x"]}}},
            "okta-sub",
            [],
        ),
    ],
)
def test_oidc_okta_claim_fallbacks(
    mock_discovery_data,
    mock_jwt,
    mock_signing_key,
    oidc_config,
    claims,
    expected_user,
    expected_roles,
):
    signing_key = MagicMock()
    signing_key.key = "a-key"
    mock_signing_key.return_value = signing_key
    mock_discovery_data.return_value = {
        "authorization_endpoint": "https://localhost:8080/realms/master/protocol/openid-connect/auth",
        "token_endpoint": "https://localhost:8080/realms/master/protocol/openid-connect/token",
        "jwks_uri": "https://localhost:8080/realms/master/protocol/openid-connect/certs",
    }
    mock_jwt.return_value = claims

    token_parser = OidcTokenParser(auth_config=oidc_config)
    user = token_parser.user_details_from_access_token_sync(access_token="aaa-bbb-ccc")

    assertpy.assert_that(user.username).is_equal_to(expected_user)
    assertpy.assert_that(user.roles).is_equal_to(expected_roles)


@patch("feast.permissions.auth.oidc_token_parser.PyJWKClient.get_signing_key_from_jwt")
@patch("feast.permissions.auth.oidc_token_parser.jwt.decode")
@patch("feast.permissions.oidc_service.OIDCDiscoveryService._fetch_discovery_data")
def test_oidc_missing_identity_claims_fail(
    mock_discovery_data, mock_jwt, mock_signing_key, oidc_config
):
    signing_key = MagicMock()
    signing_key.key = "a-key"
    mock_signing_key.return_value = signing_key
    mock_discovery_data.return_value = {
        "authorization_endpoint": "https://localhost:8080/realms/master/protocol/openid-connect/auth",
        "token_endpoint": "https://localhost:8080/realms/master/protocol/openid-connect/token",
        "jwks_uri": "https://localhost:8080/realms/master/protocol/openid-connect/certs",
    }
    mock_jwt.return_value = {"iss": "https://example"}

    token_parser = OidcTokenParser(auth_config=oidc_config)
    with pytest.raises(AuthenticationError):
        token_parser.user_details_from_access_token_sync(access_token="aaa-bbb-ccc")


@pytest.fixture(scope="module")
def rsa_keypair():
    return generate_rsa_keypair()


def _okta_auth_config() -> OidcAuthConfig:
    return OidcAuthConfig(
        type="oidc",
        auth_discovery_url="https://example/.well-known/openid-configuration",
        client_id=OKTA_CLIENT_ID,
        issuer=ISSUER,
        audience=AUDIENCE,
    )


def test_oidc_real_jwt_okta_sub_succeeds(rsa_keypair, monkeypatch):
    private_key, public_key = rsa_keypair
    patch_oidc_jwks(monkeypatch, public_key)
    token = encode_jwt(private_key)

    user = OidcTokenParser(
        auth_config=_okta_auth_config()
    ).user_details_from_access_token_sync(token)
    assertpy.assert_that(user.username).is_equal_to("okta-app")
    assertpy.assert_that(user.roles).is_equal_to([])


def test_oidc_real_jwt_wrong_audience_fails(rsa_keypair, monkeypatch):
    private_key, public_key = rsa_keypair
    patch_oidc_jwks(monkeypatch, public_key)
    token = encode_jwt(private_key, aud="wrong-audience")

    with pytest.raises(AuthenticationError):
        OidcTokenParser(
            auth_config=_okta_auth_config()
        ).user_details_from_access_token_sync(token)


def test_oidc_real_jwt_missing_issuer_fails(rsa_keypair, monkeypatch):
    private_key, public_key = rsa_keypair
    patch_oidc_jwks(monkeypatch, public_key)
    token = encode_jwt(private_key, iss=None)

    with pytest.raises(AuthenticationError):
        OidcTokenParser(
            auth_config=_okta_auth_config()
        ).user_details_from_access_token_sync(token)


def test_oidc_real_jwt_expired_fails(rsa_keypair, monkeypatch):
    private_key, public_key = rsa_keypair
    patch_oidc_jwks(monkeypatch, public_key)
    token = encode_jwt(
        private_key, exp=datetime.now(timezone.utc) - timedelta(minutes=5)
    )

    with pytest.raises(AuthenticationError):
        OidcTokenParser(
            auth_config=_okta_auth_config()
        ).user_details_from_access_token_sync(token)


def test_oidc_real_jwt_bad_signature_fails(rsa_keypair, monkeypatch):
    _, public_key = rsa_keypair
    other_private, _ = generate_rsa_keypair()
    patch_oidc_jwks(monkeypatch, public_key)
    token = encode_jwt(other_private)

    with pytest.raises(AuthenticationError):
        OidcTokenParser(
            auth_config=_okta_auth_config()
        ).user_details_from_access_token_sync(token)
