from typing import Any, Dict

from testcontainers.elasticsearch import ElasticSearchContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class ElasticSearchOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = ElasticSearchContainer(
            "elasticsearch:8.18.0",
        ).with_exposed_ports(9200)

    def create_online_store(self) -> Dict[str, Any]:
        self.container.start()
        return {
            "host": "localhost",
            "type": "elasticsearch",
            "port": self.container.get_exposed_port(9200),
            "vector_length": 2,
            "similarity": "cosine",
        }

    def teardown(self):
        self.container.stop()
