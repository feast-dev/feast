from feast.infra.offline_stores.contrib.postgres_offline_store.tests.data_source import (
    PostgreSQLDataSourceCreator,
)
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(online_store_creator=PostgreSQLDataSourceCreator),
]
