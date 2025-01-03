from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.repo_configuration import MILVUS_CONFIG
from tests.integration.feature_repos.universal.online_store.milvus import (
    MilvusOnlineStoreCreator,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        online_store="milvus", online_store_creator=MilvusOnlineStoreCreator
    ),
]

AVAILABLE_ONLINE_STORES = {"milvus": (MILVUS_CONFIG, MilvusOnlineStoreCreator)}
