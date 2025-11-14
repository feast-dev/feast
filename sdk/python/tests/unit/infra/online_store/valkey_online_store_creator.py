from typing import Dict

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class ValkeyContainer(DockerContainer):
    def __init__(self, image: str = "valkey/valkey:8.0"):
        super().__init__(image=image)
        self.with_exposed_ports(6379)
        self.port = 6379


class ValkeyOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = ValkeyContainer("valkey/valkey:8.0")

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()

        # Valkey logs the same "Server initialized" line as Redis,
        # so this predicate still works.
        log_string_to_wait_for = "Server initialized"
        wait_for_logs(
            container=self.container,
            predicate=log_string_to_wait_for,
            timeout=120,
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
