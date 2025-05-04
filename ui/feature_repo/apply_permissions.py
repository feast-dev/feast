from feast import FeatureStore
from features import (
    zipcode_features_permission,
    zipcode_source_permission,
    model_v1_permission,
    risky_features_permission,
)

store = FeatureStore(repo_path=".")

store.apply([
    zipcode_features_permission,
    zipcode_source_permission,
    model_v1_permission,
    risky_features_permission,
])

print("Permissions applied successfully!")
print("Current permissions:", store.list_permissions())
