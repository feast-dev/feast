from typing import Any, Dict

import docker
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class MilvusOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.fixed_port = 19530
        self.container = DockerContainer("milvusdb/milvus:v2.4.9").with_exposed_ports(
            self.fixed_port
        )
        self.client = docker.from_env()

    def create_online_store(self) -> Dict[str, Any]:
        self.container.start()
        # Wait for Milvus server to be ready
        # log_string_to_wait_for = "Ready to accept connections"
        log_string_to_wait_for = ""
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=30
        )
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
