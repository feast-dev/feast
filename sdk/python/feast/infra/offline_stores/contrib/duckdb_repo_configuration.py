from tests.integration.feature_repos.repo_configuration import REDIS_CONFIG
from tests.integration.feature_repos.universal.data_sources.file import (
    FileDataSourceCreator,
)
from tests.integration.feature_repos.universal.online_store.redis import (
    RedisOnlineStoreCreator,
)

AVAILABLE_OFFLINE_STORES = [
    ("local", FileDataSourceCreator),
]

AVAILABLE_ONLINE_STORES = {"redis": (REDIS_CONFIG, RedisOnlineStoreCreator)}
