from feast.infra.offline_stores.contrib.spark_offline_store.tests.data_source import (
    SparkDataSourceCreator,
)
from feast.infra.offline_stores.contrib.trino_offline_store.tests.data_source import (
    TrinoSourceCreator,
)
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(offline_store_creator=SparkDataSourceCreator),
    IntegrationTestRepoConfig(offline_store_creator=TrinoSourceCreator),
]
