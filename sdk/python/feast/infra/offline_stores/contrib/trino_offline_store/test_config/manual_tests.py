from feast.infra.offline_stores.contrib.trino_offline_store.tests.data_source import (
    TrinoSourceCreator,
)
from tests.universal.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        provider="local",
        offline_store_creator=TrinoSourceCreator,  # type: ignore
    ),
]
