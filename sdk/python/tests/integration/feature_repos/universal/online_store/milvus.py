from typing import Any, Dict

from testcontainers.milvus import MilvusContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class MilvusOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.fixed_port = 19530
        self.container = MilvusContainer("milvusdb/milvus:v2.4.4").with_exposed_ports(
            self.fixed_port
        )

    def create_online_store(self) -> Dict[str, Any]:
        self.container.start()
        # Wait for Milvus server to be ready
        host = "localhost"
        port = self.container.get_exposed_port(self.fixed_port)
        return {
            "type": "milvus",
            "host": host,
            "port": int(port),
            "index_type": "IVF_FLAT",
            "metric_type": "L2",
            "embedding_dim": 2,
            "vector_enabled": True,
            "nlist": 1,
        }

    def teardown(self):
        self.container.stop()
