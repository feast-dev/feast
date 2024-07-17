import os
from datetime import datetime
from feast import FeatureStore
import pandas as pd
from feast.feast_object import ALL_RESOURCE_TYPES
from feast.permissions.action import AuthzedAction, QUERY, WRITE, CRUD
from feast.permissions.permission import Permission
from feast.permissions.policy import RoleBasedPolicy



store = FeatureStore(repo_path=".")

perm = Permission(name="list_permissions_perm", types=ALL_RESOURCE_TYPES, with_subclasses=False,
                  policy=RoleBasedPolicy(roles=["cluster-admin"]),
                  actions=QUERY+WRITE+CRUD)
store.apply([perm])

