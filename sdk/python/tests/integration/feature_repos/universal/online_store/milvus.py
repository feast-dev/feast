from typing import Any, Dict

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class MilvusOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)

    def create_online_store(self) -> Dict[str, Any]:
        return {
            "type": "milvus",
            "path": "online_store.db",
            "index_type": "IVF_FLAT",
            "metric_type": "L2",
            "embedding_dim": 2,
            "vector_enabled": True,
            "nlist": 1,
        }
