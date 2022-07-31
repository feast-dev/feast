# from feast.infra.offline_stores.contrib.athena_offline_store.tests.data_source import AthenaDataSourceCreator

from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.data_sources.athena import (
    AthenaDataSourceCreator,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(),
    IntegrationTestRepoConfig(
        provider="aws",
        offline_store_creator=AthenaDataSourceCreator,
    ),
]

AVAILABLE_OFFLINE_STORES = [("aws", AthenaDataSourceCreator)]
