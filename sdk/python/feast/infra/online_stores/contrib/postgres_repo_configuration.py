from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.online_store.postgres import (
    PostgresOnlineStoreCreator,
    PGVectorOnlineStoreCreator
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        online_store="postgres",
        online_store_creator=PostgresOnlineStoreCreator
    ),
    IntegrationTestRepoConfig(
        online_store="pgvector",
        online_store_creator=PGVectorOnlineStoreCreator
    ),
]
