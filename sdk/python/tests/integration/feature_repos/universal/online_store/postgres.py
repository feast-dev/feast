from typing import Dict

from testcontainers.postgres import PostgresContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class PostgresOnlieStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = (
            PostgresContainer(
                "postgres:16",
                username="root",
                password="test",
                dbname="test",
                )
            .with_exposed_ports(5432)
        )

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        return {
            "host": "localhost",
            "type": "postgres",
            "user": "root",
            "password": "test",
            "database": "test",
            "port": self.container.get_exposed_port(5432),
        }

    def teardown(self):
        self.container.stop()
