from feast.feast_object import ALL_RESOURCE_TYPES
from feast.permissions.action import READ, AuthzedAction, ALL_ACTIONS
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy

admin_roles = ["feast-writer"]
user_roles = ["feast-reader"]

user_perm = Permission(
    name="feast_user_permission",
    types=ALL_RESOURCE_TYPES,
    policy=RoleBasedPolicy(roles=user_roles),
    actions=[AuthzedAction.DESCRIBE] + READ
)

admin_perm = Permission(
    name="feast_admin_permission",
    types=ALL_RESOURCE_TYPES,
    policy=RoleBasedPolicy(roles=admin_roles),
    actions=ALL_ACTIONS
)
