from typing import Any, Dict

from testcontainers.mongodb import MongoDbContainer
from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class MongoDBOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        # MongoDbContainer from testcontainers sets up authentication by default
        # with username and password from the constructor
        self.container = MongoDbContainer(
            "mongo:latest",
            username="test",
            password="test",  # pragma: allowlist secret
        ).with_exposed_ports(27017)

    def create_online_store(self) -> Dict[str, Any]:
        self.container.start()
        exposed_port = self.container.get_exposed_port(27017)
        # Include authentication in the connection string
        return {
            "type": "mongodb",
            "connection_string": f"mongodb://test:test@localhost:{exposed_port}",  # pragma: allowlist secret
        }

    def teardown(self):
        self.container.stop()
