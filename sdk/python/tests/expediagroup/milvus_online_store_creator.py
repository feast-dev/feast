import random
from typing import Dict

from milvus import default_server

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class MilvusOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = DockerContainer("milvus/milvus:2.2.12").with_exposed_ports("19530")

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        log_string_to_wait_for = "Milvus Proxy successfully initialized and ready to serve!"
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=20
        )
        exposed_port = self.container.get_exposed_port("19530")

        return {"alias": "default", "type": "milvus", "host": "localhost", "port": str(exposed_port),
                "username": "user", "password": "password"}

    def teardown(self):
        self.container.stop()
