from typing import Any, Dict

from testcontainers.mongodb import MongoDBAtlasLocalContainer, MongoDbContainer

from tests.universal.feature_repos.universal.online_store_creator import (
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


class MongoDBAtlasOnlineStoreCreator(OnlineStoreCreator):
    """OnlineStoreCreator backed by ``MongoDBAtlasLocalContainer``.

    This uses the ``mongodb/mongodb-atlas-local`` Docker image which provides
    a local Atlas deployment with support for Atlas Search and Vector Search.
    """

    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = MongoDBAtlasLocalContainer(
            "mongodb/mongodb-atlas-local:8.0.4",
        )

    def create_online_store(self) -> Dict[str, Any]:
        self.container.start()
        connection_string = self.container.get_connection_string()
        return {
            "type": "mongodb",
            "connection_string": connection_string,
            "vector_enabled": True,
        }

    def teardown(self):
        self.container.stop()
