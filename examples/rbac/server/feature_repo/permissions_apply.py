from feast import FeatureStore
from feast.feast_object import ALL_RESOURCE_TYPES
from feast.permissions.action import QUERY, AuthzedAction, ALL_ACTIONS
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy

store = FeatureStore(repo_path="")


admin_roles = ["feast-admin-role"]
user_roles = ["feast-user-role"]


user_perm = Permission(
    name="feast_user_permission",
    types=ALL_RESOURCE_TYPES,
    policy=RoleBasedPolicy(roles=user_roles),
    actions=[AuthzedAction.READ] + QUERY
)


admin_perm = Permission(
    name="feast_admin_permission",
    types=ALL_RESOURCE_TYPES,
    policy=RoleBasedPolicy(roles=admin_roles),
    actions=ALL_ACTIONS
)