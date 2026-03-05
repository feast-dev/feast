from feast.infra.offline_stores.contrib.couchbase_offline_store.tests.data_source import (
    CouchbaseColumnarDataSourceCreator,
)
from tests.universal.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.universal.feature_repos.universal.online_store.redis import (
    RedisOnlineStoreCreator,
)

REDIS_CONFIG = {"type": "redis", "connection_string": "localhost:6379,db=0"}

FULL_REPO_CONFIGS = [
    IntegrationTestRepoConfig(
        provider="aws",
        offline_store_creator=CouchbaseColumnarDataSourceCreator,
    ),
]

AVAILABLE_OFFLINE_STORES = [("aws", CouchbaseColumnarDataSourceCreator)]
AVAILABLE_ONLINE_STORES = {"redis": (REDIS_CONFIG, RedisOnlineStoreCreator)}
