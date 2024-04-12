from tests.integration.feature_repos.universal.online_store.postgres import (
    PostgresOnlieStoreCreator,
)
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        online_store="postgres", online_store_creator=PostgresOnlieStoreCreator
    ),
]
