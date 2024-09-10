import os
from unittest import mock

import assertpy
import jwt
import pytest

from feast.permissions.auth.auth_type import AuthType
from feast.permissions.auth_model import (
    KubernetesAuthConfig,
    NoAuthConfig,
    OidcClientAuthConfig,
)
from feast.permissions.client.auth_client_manager import (
    AuthenticationClientManagerFactory,
)
from feast.permissions.client.intra_comm_authentication_client_manager import (
    IntraCommAuthClientManager,
)


@mock.patch.dict(os.environ, {"INTRA_COMMUNICATION_BASE64": "server_intra_com_val"})
@pytest.mark.parametrize(
    "auth_config",
    [
        NoAuthConfig(),
        KubernetesAuthConfig(**{"type": "kubernetes"}),
        OidcClientAuthConfig(
            **{
                "type": "oidc",
                "client_id": "feast-integration-client",
                "client_secret": "feast-integration-client-secret",
                "username": "reader_writer",
                "password": "password",
                "auth_discovery_url": "KEYCLOAK_URL_PLACE_HOLDER/realms/master/.well-known/openid-configuration",
            }
        ),
    ],
)
def test_authentication_client_manager_factory(auth_config):
    authentication_client_manager_factory = AuthenticationClientManagerFactory(
        auth_config
    )

    authentication_client_manager = (
        authentication_client_manager_factory.get_auth_client_manager()
    )

    if auth_config.type not in [AuthType.KUBERNETES.value, AuthType.OIDC.value]:
        with pytest.raises(
            RuntimeError,
            match=f"No Auth client manager implemented for the auth type:{auth_config.type}",
        ):
            authentication_client_manager.get_token()
    else:
        token = authentication_client_manager.get_token()

        decoded_token = jwt.decode(token, options={"verify_signature": False})
        assertpy.assert_that(authentication_client_manager).is_type_of(
            IntraCommAuthClientManager
        )

        if AuthType.KUBERNETES.value == auth_config.type:
            assertpy.assert_that(decoded_token["sub"]).is_equal_to(
                ":::server_intra_com_val"
            )
        elif AuthType.OIDC.value in auth_config.type:
            assertpy.assert_that(decoded_token["preferred_username"]).is_equal_to(
                "server_intra_com_val"
            )
