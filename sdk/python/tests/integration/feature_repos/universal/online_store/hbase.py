from typing import Dict

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class HbaseOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = DockerContainer("harisekhon/hbase").with_exposed_ports("9090")

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        log_string_to_wait_for = (
            "Initializing Hbase Local with the following configuration:"
        )
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=10
        )
        exposed_port = self.container.get_exposed_port("9090")
        return {"type": "hbase", "host": "127.0.0.1", "port": exposed_port}

    def teardown(self):
        self.container.stop()
