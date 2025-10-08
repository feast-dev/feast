# Example permissions configuration with groups and namespaces support
# This demonstrates how to use the new group-based and namespace-based policies
# in addition to the existing role-based policies

from feast.feast_object import ALL_FEATURE_VIEW_TYPES, ALL_RESOURCE_TYPES
from feast.project import Project
from feast.entity import Entity
from feast.feature_view import FeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.batch_feature_view import BatchFeatureView
from feast.stream_feature_view import StreamFeatureView
from feast.feature_service import FeatureService
from feast.data_source import DataSource
from feast.saved_dataset import SavedDataset
from feast.permissions.permission import Permission
from feast.permissions.action import READ, AuthzedAction, ALL_ACTIONS
from feast.permissions.policy import RoleBasedPolicy, GroupBasedPolicy, NamespaceBasedPolicy, CombinedGroupNamespacePolicy

# New Testing 

WITHOUT_DATA_SOURCE = [Project, Entity, FeatureService, SavedDataset] + ALL_FEATURE_VIEW_TYPES

ONLY_ENTITIES = [Entity]

ONLY_DS = [DataSource]

# Define K8s roles (existing functionality)
admin_roles = ["feast-writer"]  # Full access (can create, update, delete) Feast Resources
user_roles = ["feast-reader"]   # Read-only access on Feast Resources

# Define groups for different teams
data_team_groups = ["data-team", "ml-engineers"]
dev_team_groups = ["dev-team", "developers"]
admin_groups = ["feast-admins", "platform-admins"]

# Define namespaces for different environments
prod_namespaces = ["feast"]

# pre_changed = Permission(name="entity_reader", types=ONLY_ENTITIES, policy=NamespaceBasedPolicy(namespaces=prod_namespaces), actions=[AuthzedAction.DESCRIBE] + READ)
only_entities = Permission(
    name="pre_Changed",
    types=ONLY_ENTITIES,
    policy=NamespaceBasedPolicy(namespaces=prod_namespaces),
    actions=[AuthzedAction.DESCRIBE] + READ
)
only_ds = Permission(name="entity_reader", types=ONLY_DS, policy=NamespaceBasedPolicy(namespaces=[prod_namespaces]), actions=[AuthzedAction.DESCRIBE] + READ)
staging_namespaces = ["staging", "dev"]
test_namespaces = ["test", "testing"]

# Role-based permissions (existing functionality)
# - Grants read and describing Feast objects access
user_perm = Permission(
    name="feast_user_permission",
    types=ALL_RESOURCE_TYPES,
    policy=RoleBasedPolicy(roles=user_roles),
    actions=[AuthzedAction.DESCRIBE] + READ  # Read access (READ_ONLINE, READ_OFFLINE) + describe other Feast Resources.
)

# Admin permissions (existing functionality)
# - Grants full control over all resources
admin_perm = Permission(
    name="feast_admin_permission",
    types=ALL_RESOURCE_TYPES,
    policy=RoleBasedPolicy(roles=admin_roles),
    actions=ALL_ACTIONS  # Full permissions: CREATE, UPDATE, DELETE, READ, WRITE
)

# Group-based permissions (new functionality)
# - Grants read access to data team members
data_team_perm = Permission(
    name="data_team_read_permission",
    types=ALL_RESOURCE_TYPES,
    policy=GroupBasedPolicy(groups=data_team_groups),
    actions=[AuthzedAction.DESCRIBE] + READ
)

# - Grants full access to admin groups
admin_group_perm = Permission(
    name="admin_group_permission",
    types=ALL_RESOURCE_TYPES,
    policy=GroupBasedPolicy(groups=admin_groups),
    actions=ALL_ACTIONS
)

# Namespace-based permissions (new functionality)
# - Grants read access to production namespace users
prod_read_perm = Permission(
    name="production_read_permission",
    types=ALL_RESOURCE_TYPES,
    policy=NamespaceBasedPolicy(namespaces=prod_namespaces),
    actions=[AuthzedAction.DESCRIBE] + READ
)

# # - Grants full access to staging namespace users
staging_full_perm = Permission(
    name="staging_full_permission",
    types=ALL_RESOURCE_TYPES,
    policy=NamespaceBasedPolicy(namespaces=staging_namespaces),
    actions=ALL_ACTIONS
)

# # Combined permissions (using combined policy type)
# # - Grants read access to dev team members in test namespaces
dev_test_perm = Permission(
    name="dev_test_permission",
    types=ALL_RESOURCE_TYPES,
    policy=CombinedGroupNamespacePolicy(groups=dev_team_groups, namespaces=test_namespaces),
    actions=[AuthzedAction.DESCRIBE] + READ
)

# # - Grants full access to data team members in staging namespaces
data_staging_perm = Permission(
    name="data_staging_permission",
    types=ALL_RESOURCE_TYPES,
    policy=CombinedGroupNamespacePolicy(groups=data_team_groups, namespaces=staging_namespaces),
    actions=ALL_ACTIONS
)
