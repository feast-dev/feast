import os

from tests.universal.feature_repos.universal.data_sources.file import (
    FileDataSourceCreator,
)

MILVUS_CONFIG = {"type": "milvus", "embedding_dim": 2, "path": "online_store.db"}

AVAILABLE_OFFLINE_STORES = [("local", FileDataSourceCreator)]
AVAILABLE_ONLINE_STORES = {"milvus": (MILVUS_CONFIG, None)}

if os.getenv("FEAST_LOCAL_ONLINE_CONTAINER", "False").lower() == "true":
    from tests.universal.feature_repos.universal.online_store.milvus import (
        MilvusOnlineStoreCreator,
    )

    AVAILABLE_ONLINE_STORES["milvus"] = (MILVUS_CONFIG, MilvusOnlineStoreCreator)
