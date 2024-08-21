# test_token_validator.py

import asyncio
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
