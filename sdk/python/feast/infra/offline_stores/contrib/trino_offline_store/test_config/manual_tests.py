from feast.infra.offline_stores.contrib.trino_offline_store.tests.data_source import (
    TrinoSourceCreator,
)
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        provider="local",
        offline_store_creator=TrinoSourceCreator,  # type: ignore
    ),
]
