from feast import FeatureStore
from feast.feast_object import ALL_RESOURCE_TYPES
from feast.permissions.action import QUERY, WRITE, CRUD
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy

store = FeatureStore(repo_path=".")

create_roles = ["feast-create-role"]
read_roles = ["feast-read-role"]
update_roles = ["feast-update-role"]
delete_roles = ["feast-delete-role"]
query_roles = ["feast-query-online-role", "feast-query-offline-role"]
write_roles = ["feast-write-online-role", "feast-write-offline-role"]

#  all roles
all_roles = create_roles + read_roles + update_roles + delete_roles + query_roles + write_roles

create_perm = Permission(
    name="feast_create_permission",
    types=ALL_RESOURCE_TYPES,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=create_roles),
    actions=CRUD
)

read_perm = Permission(
    name="feast_read_permission",
    types=ALL_RESOURCE_TYPES,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=read_roles),
    actions=QUERY
)

update_perm = Permission(
    name="feast_update_permission",
    types=ALL_RESOURCE_TYPES,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=update_roles),
    actions=CRUD
)

delete_perm = Permission(
    name="feast_delete_permission",
    types=ALL_RESOURCE_TYPES,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=delete_roles),
    actions=CRUD
)

query_perm = Permission(
    name="feast_query_permission",
    types=ALL_RESOURCE_TYPES,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=query_roles),
    actions=QUERY
)

write_perm = Permission(
    name="feast_write_permission",
    types=ALL_RESOURCE_TYPES,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=write_roles),
    actions=WRITE
)

full_access_perm = Permission(
    name="feast_full_access_permission",
    types=ALL_RESOURCE_TYPES,
    with_subclasses=False,
    policy=RoleBasedPolicy(roles=all_roles),
    actions=QUERY + WRITE + CRUD
)

store.apply([full_access_perm])
