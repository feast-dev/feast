from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.online_store.mysql import (
    BatchWriteMySQLOnlineStoreCreator,
    MySQLOnlineStoreCreator,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(online_store_creator=MySQLOnlineStoreCreator),
    IntegrationTestRepoConfig(online_store_creator=BatchWriteMySQLOnlineStoreCreator),
]
