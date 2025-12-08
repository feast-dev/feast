from feast.feast_object import ALL_FEATURE_VIEW_TYPES
from feast.permissions.permission import Permission
from feast.permissions.action import READ, AuthzedAction
from feast.permissions.policy import NamespaceBasedPolicy
from feast.project import Project
from feast.entity import Entity
from feast.feature_service import FeatureService
from feast.saved_dataset import SavedDataset

perm_namespace = ["test-ns-feast"]

WITHOUT_DATA_SOURCE = [Project, Entity, FeatureService, SavedDataset] + ALL_FEATURE_VIEW_TYPES

test_perm = Permission(
    name="feast-auth",
    types=WITHOUT_DATA_SOURCE,
    policy=NamespaceBasedPolicy(namespaces=perm_namespace),
    actions=[AuthzedAction.DESCRIBE] + READ
)
