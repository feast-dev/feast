from feast.infra.offline_stores.contrib.spark_offline_store.tests.data_source import (
    SparkDataSourceCreator,
)
from tests.integration.feature_repos.repo_configuration import REDIS_CONFIG
from tests.integration.feature_repos.universal.online_store.redis import (
    RedisOnlineStoreCreator,
)

AVAILABLE_OFFLINE_STORES = [
    ("local", SparkDataSourceCreator),
]

AVAILABLE_ONLINE_STORES = {"redis": (REDIS_CONFIG, RedisOnlineStoreCreator)}
