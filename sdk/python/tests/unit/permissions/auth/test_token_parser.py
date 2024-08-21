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
def test_oidc_token_validation_success(
    mock_jwt, mock_signing_key, mock_oauth2, oidc_config
):
    signing_key = MagicMock()
    signing_key.key = "a-key"
    mock_signing_key.return_value = signing_key

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
@pytest.mark.parametrize("intra_communication_val", ("test1234", "my-name"))
def test_oidc_inter_server_comm(intra_communication_val, oidc_config, monkeypatch):
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

    if intra_communication_val != "test1234":
        user_data["resource_access"] = {_CLIENT_ID: {"roles": ["reader", "writer"]}}

    monkeypatch.setattr(
        "feast.permissions.auth.oidc_token_parser.jwt.decode",
        lambda self, *args, **kwargs: user_data,
    )

    access_token = "aaa-bbb-ccc"
    token_parser = OidcTokenParser(auth_config=oidc_config)
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token=access_token)
    )

    if intra_communication_val == "test1234":
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
@patch(
    "feast.permissions.auth.kubernetes_token_parser.client.RbacAuthorizationV1Api.list_cluster_role_binding"
)
def test_k8s_token_validation_success(
    mock_crb,
    mock_rb,
    mock_jwt,
    mock_config,
    rolebindings,
    clusterrolebindings,
):
    sa_name = "my-name"
    namespace = "my-ns"
    subject = f"system:serviceaccount:{namespace}:{sa_name}"
    mock_jwt.return_value = {"sub": subject}

    mock_rb.return_value = rolebindings["items"]
    mock_crb.return_value = clusterrolebindings["items"]

    roles = rolebindings["roles"]
    croles = clusterrolebindings["roles"]

    access_token = "aaa-bbb-ccc"
    token_parser = KubernetesTokenParser()
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token=access_token)
    )

    assertpy.assert_that(user).is_type_of(User)
    if isinstance(user, User):
        assertpy.assert_that(user.username).is_equal_to(f"{namespace}:{sa_name}")
        assertpy.assert_that(user.roles.sort()).is_equal_to((roles + croles).sort())
        for r in roles:
            assertpy.assert_that(user.has_matching_role([r])).is_true()
        for cr in croles:
            assertpy.assert_that(user.has_matching_role([cr])).is_true()
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
@pytest.mark.parametrize("intra_communication_val", ("test1234", "my-name"))
def test_k8s_inter_server_comm(
    intra_communication_val,
    oidc_config,
    request,
    rolebindings,
    clusterrolebindings,
    monkeypatch,
):
    if intra_communication_val == "test1234":
        subject = f":::{intra_communication_val}"
    else:
        sa_name = request.getfixturevalue("sa_name")
        namespace = request.getfixturevalue("namespace")
        subject = f"system:serviceaccount:{namespace}:{sa_name}"
        rolebindings = request.getfixturevalue("rolebindings")
        clusterrolebindings = request.getfixturevalue("clusterrolebindings")

        monkeypatch.setattr(
            "feast.permissions.auth.kubernetes_token_parser.client.RbacAuthorizationV1Api.list_namespaced_role_binding",
            lambda *args, **kwargs: rolebindings["items"],
        )
        monkeypatch.setattr(
            "feast.permissions.auth.kubernetes_token_parser.client.RbacAuthorizationV1Api.list_cluster_role_binding",
            lambda *args, **kwargs: clusterrolebindings["items"],
        )
        monkeypatch.setattr(
            "feast.permissions.client.kubernetes_auth_client_manager.KubernetesAuthClientManager.get_token",
            lambda self: "my-token",
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
    croles = clusterrolebindings["roles"]

    access_token = "aaa-bbb-ccc"
    token_parser = KubernetesTokenParser()
    user = asyncio.run(
        token_parser.user_details_from_access_token(access_token=access_token)
    )

    if intra_communication_val == "test1234":
        assertpy.assert_that(user).is_not_none()
        assertpy.assert_that(user.username).is_equal_to(intra_communication_val)
        assertpy.assert_that(user.roles).is_equal_to([])
    else:
        assertpy.assert_that(user).is_type_of(User)
        if isinstance(user, User):
            assertpy.assert_that(user.username).is_equal_to(f"{namespace}:{sa_name}")
            assertpy.assert_that(user.roles.sort()).is_equal_to((roles + croles).sort())
            for r in roles:
                assertpy.assert_that(user.has_matching_role([r])).is_true()
            for cr in croles:
                assertpy.assert_that(user.has_matching_role([cr])).is_true()
            assertpy.assert_that(user.has_matching_role(["foo"])).is_false()
