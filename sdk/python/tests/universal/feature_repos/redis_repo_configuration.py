import os

from tests.universal.feature_repos.universal.data_sources.file import (
    FileDataSourceCreator,
)

REDIS_CONFIG = {"type": "redis", "connection_string": "localhost:6379,db=0"}

AVAILABLE_OFFLINE_STORES = [("local", FileDataSourceCreator)]
AVAILABLE_ONLINE_STORES = {"redis": (REDIS_CONFIG, None)}

if os.getenv("FEAST_LOCAL_ONLINE_CONTAINER", "False").lower() == "true":
    from tests.universal.feature_repos.universal.online_store.redis import (
        RedisOnlineStoreCreator,
    )

    AVAILABLE_ONLINE_STORES["redis"] = (REDIS_CONFIG, RedisOnlineStoreCreator)
