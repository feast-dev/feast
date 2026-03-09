from tests.universal.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.universal.feature_repos.universal.online_store.milvus import (
    MilvusOnlineStoreCreator,
)

MILVUS_CONFIG = {"type": "milvus", "embedding_dim": 2, "path": "online_store.db"}

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        online_store="milvus", online_store_creator=MilvusOnlineStoreCreator
    ),
]

AVAILABLE_ONLINE_STORES = {"milvus": (MILVUS_CONFIG, MilvusOnlineStoreCreator)}
