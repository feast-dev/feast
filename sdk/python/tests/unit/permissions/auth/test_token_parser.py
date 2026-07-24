import asyncio
import os
from unittest import mock
from unittest.mock import MagicMock, patch

import assertpy
import pytest
from starlette.authentication import (
    AuthenticationError,
)

from feast.permissions.auth.kubernetes_token_parser import KubernetesTokenParser
from feast.permissions.auth.oidc_token_parser import OidcTokenParser
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
