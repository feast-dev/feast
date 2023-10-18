import logging
import random
import string
from typing import Any, Dict

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class HazelcastOnlineStoreCreator(OnlineStoreCreator):

    cluster_name: str = ""
    container: DockerContainer = None

    def __init__(self, project_name: str, **kwargs):
        logging.getLogger("hazelcast").setLevel(logging.ERROR)
        super().__init__(project_name)
        self.cluster_name = "".join(
            random.choice(string.ascii_lowercase) for _ in range(5)
        )
        self.container = (
            DockerContainer("hazelcast/hazelcast")
            .with_env("HZ_CLUSTERNAME", self.cluster_name)
            .with_env("HZ_NETWORK_PORT_AUTOINCREMENT", "true")
            .with_exposed_ports(5701)
        )

    def create_online_store(self) -> Dict[str, Any]:
        self.container.start()
        cluster_member = (
            self.container.get_container_host_ip()
            + ":"
            + self.container.get_exposed_port(5701)
        )
        log_string_to_wait_for = r"Cluster name: " + self.cluster_name
        wait_for_logs(self.container, predicate=log_string_to_wait_for, timeout=10)
        return {
            "type": "hazelcast",
            "cluster_name": self.cluster_name,
            "cluster_members": [cluster_member],
        }

    def teardown(self):
        self.container.stop()
