import pytest
from kubernetes import client

from feast.permissions.auth_model import OidcAuthConfig
from tests.unit.permissions.auth.server.test_utils import (
    invalid_list_entities_perm,
    read_entities_perm,
    read_fv_perm,
    read_odfv_perm,
    read_permissions_perm,
    read_sfv_perm,
)
from tests.unit.permissions.auth.test_token_parser import _CLIENT_ID


@pytest.fixture
def sa_name():
    return "my-name"


@pytest.fixture
def namespace():
    return "my-ns"


@pytest.fixture
def rolebindings(sa_name, namespace) -> dict:
    roles = ["reader", "writer"]
    items = []
    for r in roles:
        items.append(
            client.V1RoleBinding(
                metadata=client.V1ObjectMeta(name=r, namespace=namespace),
                subjects=[
                    client.V1Subject(
                        kind="ServiceAccount",
                        name=sa_name,
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
def clusterrolebindings(sa_name, namespace) -> dict:
    roles = ["updater"]
    items = []
    for r in roles:
        items.append(
            client.V1ClusterRoleBinding(
                metadata=client.V1ObjectMeta(name=r, namespace=namespace),
                subjects=[
                    client.V1Subject(
                        kind="ServiceAccount",
                        name=sa_name,
                        namespace=namespace,
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
        auth_server_url="",
        auth_discovery_url="",
        client_id=_CLIENT_ID,
        client_secret="",
        username="",
        password="",
        realm="",
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
        ],
    ],
)
def applied_permissions(request):
    return request.param
