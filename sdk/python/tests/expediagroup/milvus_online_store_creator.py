from typing import Dict

from milvus import default_server

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class MilvusOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.host = "localhost"
        self.port = 19530
        self.server = default_server
        self.server.wait_for_started = False

    def create_online_store(self) -> Dict[str, str]:
        self.server.start()

        return {"type": "milvus", "host": self.host, "port": str(self.port)}

    def teardown(self):
        self.server.stop()
