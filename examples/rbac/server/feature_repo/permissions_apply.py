from feast import FeatureStore
from feast.feast_object import ALL_RESOURCE_TYPES
from feast.permissions.action import QUERY, AuthzedAction, ALL_ACTIONS
from feast.permissions.decision import DecisionStrategy
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy

store = FeatureStore(repo_path="")


admin_roles = ["feast-admin-role"]
user_roles = ["feast-user-role"]
cluster_roles = ["feast-cluster-role"]


user_perm = Permission(
    name="feast_user_permission",
    types=ALL_RESOURCE_TYPES,
    policy=RoleBasedPolicy(roles=user_roles + admin_roles + cluster_roles),
    actions=[AuthzedAction.READ] + QUERY
)


admin_perm = Permission(
    name="feast_admin_permission",
    types=ALL_RESOURCE_TYPES,
    policy=RoleBasedPolicy(roles=admin_roles + cluster_roles),
    actions=ALL_ACTIONS
)


#Permission.set_global_decision_strategy(DecisionStrategy.AFFIRMATIVE)
#store.apply([user_perm, admin_perm])
#store.refresh_registry()
#print("Global decision strategy:", Permission.get_global_decision_strategy())
