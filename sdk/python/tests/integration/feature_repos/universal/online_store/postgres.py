from typing import Dict

from testcontainers.postgres import PostgresContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class PostgresOnlieStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = (
            PostgresContainer("postgres:16", platform="linux/amd64")
            .with_exposed_ports(5432)
            .with_env("POSTGRES_USER", "root")
            .with_env("POSTGRES_PASSWORD", "test")
            .with_env("POSTGRES_DB", "test")
        )

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        exposed_port = self.container.get_exposed_port(5432)
        return {
            "type": "postgres",
            "user": "root",
            "password": "test",
            "database": "test",
            "port": exposed_port,
        }

    def teardown(self):
        self.container.stop()
