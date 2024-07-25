from textwrap import dedent

import pytest

from tests.utils.auth_permissions_util import (
    invalid_list_entities_perm,
    list_entities_perm,
    list_fv_perm,
    list_odfv_perm,
    list_permissions_perm,
    list_sfv_perm,
)


@pytest.fixture(
    scope="module",
    params=[
        dedent("""
          auth:
            type: no_auth
          """),
        dedent("""
          auth:
            type: kubernetes
        """),
        dedent("""
          auth:
            type: oidc
            client_id: client_id
            client_secret: client_secret
            username: username
            password: password
            realm: realm
            auth_server_url: http://localhost:8080
            auth_discovery_url: http://localhost:8080/realms/master/.well-known/openid-configuration
        """),
    ],
)
def auth_config(request):
    return request.param


@pytest.fixture(
    scope="module",
    params=[
        [],
        [invalid_list_entities_perm],
        [
            list_entities_perm,
            list_permissions_perm,
            list_fv_perm,
            list_odfv_perm,
            list_sfv_perm,
        ],
    ],
)
def applied_permissions(request):
    return request.param
