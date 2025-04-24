from typing import Any, Dict

from testcontainers.qdrant import QdrantContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class QdrantOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = QdrantContainer(
            "qdrant/qdrant",
        )

    def create_online_store(self) -> Dict[str, Any]:
        self.container.start()
        return {
            "host": self.container.get_container_host_ip(),
            "type": "qdrant",
            "port": self.container.exposed_rest_port,
            "vector_length": 2,
            "similarity": "cosine",
        }

    def teardown(self):
        self.container.stop()
