from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.online_store.duckdb import (
    DuckDBOnlineStoreCreator,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        online_store="duckdb", online_store_creator=DuckDBOnlineStoreCreator
    ),
]
