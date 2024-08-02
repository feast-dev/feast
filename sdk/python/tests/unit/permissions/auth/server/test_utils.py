import assertpy
import pytest

from feast import Entity, FeatureView, OnDemandFeatureView, StreamFeatureView
from feast.permissions.action import AuthzedAction
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.permissions.server.utils import AuthManagerType, str_to_auth_manager_type

read_permissions_perm = Permission(
    name="read_permissions_perm",
    types=Permission,
    policy=RoleBasedPolicy(roles=["reader"]),
    actions=[AuthzedAction.READ],
)

read_entities_perm = Permission(
    name="read_entities_perm",
    types=Entity,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=["reader"]),
    actions=[AuthzedAction.READ],
)

read_fv_perm = Permission(
    name="read_fv_perm",
    types=FeatureView,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=["reader"]),
    actions=[AuthzedAction.READ],
)

read_odfv_perm = Permission(
    name="read_odfv_perm",
    types=OnDemandFeatureView,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=["reader"]),
    actions=[AuthzedAction.READ],
)

read_sfv_perm = Permission(
    name="read_sfv_perm",
    types=StreamFeatureView,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=["reader"]),
    actions=[AuthzedAction.READ],
)

invalid_list_entities_perm = Permission(
    name="invalid_list_entity_perm",
    types=Entity,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=["dancer"]),
    actions=[AuthzedAction.READ],
)


@pytest.mark.parametrize(
    "label, value",
    [(t.value, t) for t in AuthManagerType]
    + [(t.value.upper(), t) for t in AuthManagerType]
    + [(t.value.lower(), t) for t in AuthManagerType]
    + [("none", AuthManagerType.NONE)],
)
def test_str_to_auth_type(label, value):
    assertpy.assert_that(str_to_auth_manager_type(label)).is_equal_to(value)
