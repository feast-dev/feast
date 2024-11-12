import time
from typing import Any, Dict

from pymilvus import connections
from testcontainers.core.container import DockerContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class MilvusOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = DockerContainer("milvusdb/milvus:v2.2.9").with_exposed_ports(
            19530
        )

    def create_online_store(self) -> Dict[str, Any]:
        self.container.start()
        # Wait for Milvus server to be ready
        host = "localhost"
        port = self.container.get_exposed_port(19530)

        max_attempts = 12
        for attempt in range(1, max_attempts + 1):
            try:
                print(
                    f"Attempting to connect to Milvus at {host}:{port}, attempt {attempt}"
                )
                connections.connect(alias="default", host=host, port=port)
                if connections.has_connection(alias="default"):
                    print("Successfully connected to Milvus")
                    break
            except Exception as e:
                print(f"Connection attempt failed: {e}")
                time.sleep(5)
        else:
            raise RuntimeError(
                "Cannot connect to Milvus server after multiple attempts"
            )

        return {
            "type": "milvus",
            "host": host,
            "port": int(port),
            "index_type": "IVF_FLAT",
            "metric_type": "L2",
            "embedding_dim": 128,  # Adjust based on your embedding dimension
            "vector_enabled": True,
        }

    def teardown(self):
        self.container.stop()
