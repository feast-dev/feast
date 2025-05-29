# Necessary modules for permissions and policies in Feast for RBAC
from feast.feast_object import ALL_RESOURCE_TYPES
from feast.permissions.action import READ, AuthzedAction, ALL_ACTIONS
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy

# Define K8s roles same as created with FeatureStore CR
admin_roles = ["feast-writer"]  # Full access (can create, update, delete ) Feast Resources
user_roles = ["feast-reader"]   # Read-only access on Feast Resources

# User permissions (feast_user_permission)
# - Grants read and describing Feast objects access
user_perm = Permission(
    name="feast_user_permission",
    types=ALL_RESOURCE_TYPES,
    policy=RoleBasedPolicy(roles=user_roles),
    actions=[AuthzedAction.DESCRIBE] + READ  # Read access (READ_ONLINE, READ_OFFLINE) + describe other Feast Resources.
)

# Admin permissions (feast_admin_permission)
# - Grants full control over all resources
admin_perm = Permission(
    name="feast_admin_permission",
    types=ALL_RESOURCE_TYPES,
    policy=RoleBasedPolicy(roles=admin_roles),
    actions=ALL_ACTIONS  # Full permissions: CREATE, UPDATE, DELETE, READ, WRITE
)
