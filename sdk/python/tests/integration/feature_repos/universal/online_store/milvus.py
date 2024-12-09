from typing import Any, Dict

from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.milvus import MilvusContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class MilvusOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = MilvusContainer("milvusdb/milvus:v2.4.4").with_exposed_ports(
            19530
        )

    def create_online_store(self) -> Dict[str, Any]:
        self.container.start()
        # Wait for Milvus server to be ready
        host = "localhost"
        port = self.container.get_exposed_port(19530)
        return {
            "type": "milvus",
            "host": host,
            "port": int(port),
            "index_type": "IVF_FLAT",
            "metric_type": "L2",
            "embedding_dim": 128,
            "vector_enabled": True,
        }

    def teardown(self):
        self.container.stop()
