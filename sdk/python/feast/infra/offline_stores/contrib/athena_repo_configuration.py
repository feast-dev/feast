from feast.infra.offline_stores.contrib.athena_offline_store.tests.data_source import (
    AthenaDataSourceCreator,
)
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        provider="aws",
        offline_store_creator=AthenaDataSourceCreator,
    ),
]

AVAILABLE_OFFLINE_STORES = [("aws", AthenaDataSourceCreator)]
