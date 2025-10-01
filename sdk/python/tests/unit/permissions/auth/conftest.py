import pytest
from kubernetes import client

from feast.permissions.auth_model import OidcAuthConfig
from tests.unit.permissions.auth.server.test_utils import (
    invalid_list_entities_perm,
    read_entities_perm,
    read_fv_perm,
    read_odfv_perm,
    read_permissions_perm,
    read_projects_perm,
    read_sfv_perm,
)
from tests.unit.permissions.auth.test_token_parser import _CLIENT_ID


@pytest.fixture
def sa_name():
    return "my-name"


@pytest.fixture
def my_namespace():
    return "my-ns"


@pytest.fixture
def sa_namespace():
    return "sa-ns"


@pytest.fixture
def rolebindings(my_namespace, sa_name, sa_namespace) -> dict:
    roles = ["reader", "writer"]
    items = []
    for r in roles:
        items.append(
            client.V1RoleBinding(
                metadata=client.V1ObjectMeta(name=r, namespace=my_namespace),
                subjects=[
                    client.RbacV1Subject(
                        kind="ServiceAccount",
                        name=sa_name,
                        namespace=sa_namespace,
                        api_group="rbac.authorization.k8s.io",
                    )
                ],
                role_ref=client.V1RoleRef(
                    kind="Role", name=r, api_group="rbac.authorization.k8s.io"
                ),
            )
        )
    return {"items": client.V1RoleBindingList(items=items), "roles": roles}


@pytest.fixture
def oidc_config() -> OidcAuthConfig:
    return OidcAuthConfig(
        auth_discovery_url="https://localhost:8080/realms/master/.well-known/openid-configuration",
        client_id=_CLIENT_ID,
        type="oidc",
    )


@pytest.fixture(
    scope="module",
    params=[
        [],
        [invalid_list_entities_perm],
        [
            read_entities_perm,
            read_permissions_perm,
            read_fv_perm,
            read_odfv_perm,
            read_sfv_perm,
            read_projects_perm,
        ],
    ],
)
def applied_permissions(request):
    return request.param
