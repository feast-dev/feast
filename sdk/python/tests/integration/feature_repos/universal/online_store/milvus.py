from typing import Any

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class MilvusOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.db_path = "online_store.db"

    def create_online_store(self) -> dict[str, Any]:
        return {
            "type": "milvus",
            "path": self.db_path,
            "index_type": "IVF_FLAT",
            "metric_type": "L2",
            "embedding_dim": 2,
            "vector_enabled": True,
            "nlist": 1,
        }

    def teardown(self):
        """Clean up Milvus online store resources."""
        import os

        if os.path.exists(self.db_path):
            try:
                os.remove(self.db_path)
            except Exception:
                pass
