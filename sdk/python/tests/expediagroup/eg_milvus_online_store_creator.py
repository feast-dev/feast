from typing import Dict

from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.milvus import MilvusContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class EGMilvusOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        with MilvusContainer("milvusdb/milvus:v2.4.11") as milvus_container:
            self.container = milvus_container

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        log_string_to_wait_for = (
            "Milvus Proxy successfully initialized and ready to serve!"
        )
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=60
        )
        host = self.container.get_container_host_ip()
        exposed_port = int(self.container.get_exposed_port(self.container.port))

        return {
            "alias": "default",
            "type": "eg-milvus",
            "host": host,
            "port": exposed_port,
            "username": "user",
            "password": "password",
        }

    def teardown(self):
        self.container.stop()
