from typing import Dict

from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.redis import RedisContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class RedisOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        with RedisContainer("redis:latest") as redis_container:
            self.container = redis_container

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        log_string_to_wait_for = "Server initialized"
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=120
        )
        host = self.container.get_container_host_ip()
        exposed_port = int(self.container.get_exposed_port(self.container.port))
        connection_string = f"{host}:{exposed_port}"
        print(f"connection_string: {connection_string}")
        return {
            "connection_string": connection_string,
        }

    def teardown(self):
        self.container.stop()