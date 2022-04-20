from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.data_sources.postgres import (
    PostgreSQLDataSourceCreator,
)

POSTGRES_ONLINE_CONFIG = {
    "type": "postgres",
    "host": "localhost",
    "port": "5432",
    "database": "postgres",
    "db_schema": "feature_store",
    "user": "postgres",
    "password": "docker",
}

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        provider="local",
        offline_store_creator=PostgreSQLDataSourceCreator,
        online_store=POSTGRES_ONLINE_CONFIG,
    ),
]
