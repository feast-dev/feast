from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.data_sources.trino import (
    TrinoSourceCreator,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        provider="local", offline_store_creator=TrinoSourceCreator,
    ),
]
