from feast import FeatureStore
from features import (
    zipcode_features_permission,
    zipcode_source_permission,
    model_v1_permission,
    risky_features_permission,
    document_embeddings_permission,
    document_metadata_permission,
    rag_model_permission,
)

store = FeatureStore(repo_path=".")

store.apply([
    zipcode_features_permission,
    zipcode_source_permission,
    model_v1_permission,
    risky_features_permission,
    document_embeddings_permission,
    document_metadata_permission,
    rag_model_permission,
])

print("Permissions applied successfully!")
print("Current permissions:", store.list_permissions())
