from typing import Dict

from testcontainers.mysql import MySqlContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class MySQLOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = (
            MySqlContainer("mysql:latest", platform="linux/amd64")
            .with_exposed_ports(3306)
            .with_env("MYSQL_USER", "root")
            .with_env("MYSQL_PASSWORD", "test")
            .with_env("MYSQL_DATABASE", "test")
        )

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        exposed_port = self.container.get_exposed_port(3306)
        return {
            "type": "mysql",
            "user": "root",
            "password": "test",
            "database": "test",
            "port": exposed_port,
        }

    def teardown(self):
        self.container.stop()
