from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.data_sources.postgres_data_source import (
    PostgreSQLDataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.spark_data_source_creator import (
    SparkDataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.trino import (
    TrinoSourceCreator,
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
    IntegrationTestRepoConfig(offline_store_creator=SparkDataSourceCreator),
    IntegrationTestRepoConfig(offline_store_creator=TrinoSourceCreator),
    IntegrationTestRepoConfig(
        provider="local",
        offline_store_creator=PostgreSQLDataSourceCreator,
        online_store=POSTGRES_ONLINE_CONFIG,
    ),
]
