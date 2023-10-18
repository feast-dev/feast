from typing import Dict

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class DynamoDBOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = DockerContainer(
            "amazon/dynamodb-local:latest"
        ).with_exposed_ports("8000")

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        log_string_to_wait_for = (
            "Initializing DynamoDB Local with the following configuration:"
        )
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=10
        )
        exposed_port = self.container.get_exposed_port("8000")
        return {
            "type": "dynamodb",
            "endpoint_url": f"http://localhost:{exposed_port}",
            "region": "us-west-2",
        }

    def teardown(self):
        self.container.stop()
