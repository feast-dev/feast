from feast.infra.offline_stores.contrib.couchbase_offline_store.tests.data_source import (
    CouchbaseColumnarDataSourceCreator,
)
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.repo_configuration import REDIS_CONFIG
from tests.integration.feature_repos.universal.online_store.redis import (
    RedisOnlineStoreCreator,
)

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        provider="aws",
        offline_store_creator=CouchbaseColumnarDataSourceCreator,
    ),
]

AVAILABLE_OFFLINE_STORES = [("aws", CouchbaseColumnarDataSourceCreator)]
AVAILABLE_ONLINE_STORES = {"redis": (REDIS_CONFIG, RedisOnlineStoreCreator)}
