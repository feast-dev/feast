from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.online_store.mongodb import (
    MongoDBOnlineStoreCreator,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        online_store="mongodb",
        online_store_creator=MongoDBOnlineStoreCreator,
    ),
]
