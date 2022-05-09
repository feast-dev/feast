from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.data_sources.postgres import (
    PostgreSQLDataSourceCreator,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        provider="local",
        offline_store_creator=PostgreSQLDataSourceCreator,
        online_store_creator=PostgreSQLDataSourceCreator,
    ),
]
