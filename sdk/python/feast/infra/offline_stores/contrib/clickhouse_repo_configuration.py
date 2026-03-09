from feast.infra.offline_stores.contrib.clickhouse_offline_store.tests.data_source import (
    ClickhouseDataSourceCreator,
)
from tests.universal.feature_repos.universal.online_store.redis import (
    RedisOnlineStoreCreator,
)

REDIS_CONFIG = {"type": "redis", "connection_string": "localhost:6379,db=0"}

AVAILABLE_OFFLINE_STORES = [("local", ClickhouseDataSourceCreator)]

AVAILABLE_ONLINE_STORES = {"redis": (REDIS_CONFIG, RedisOnlineStoreCreator)}
