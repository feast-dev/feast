import os
from typing import Any, Dict

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.postgres import PostgresContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class PostgresOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.container = PostgresContainer(
            "postgres:16",
            username="root",
            password="test!@#$%",
            dbname="test",
        ).with_exposed_ports(5432)

    def create_online_store(self) -> Dict[str, str]:
        self.container.start()
        return {
            "host": "localhost",
            "type": "postgres",
            "user": "root",
            "password": "test!@#$%",
            "database": "test",
            "port": self.container.get_exposed_port(5432),
        }

    def teardown(self):
        self.container.stop()


class PGVectorOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        script_directory = os.path.dirname(os.path.abspath(__file__))
        self.container = (
            DockerContainer("pgvector/pgvector:pg16")
            .with_env("POSTGRES_USER", "root")
            .with_env("POSTGRES_PASSWORD", "test!@#$%")
            .with_env("POSTGRES_DB", "test")
            .with_exposed_ports(5432)
            .with_volume_mapping(
                os.path.join(script_directory, "init.sql"),
                "/docker-entrypoint-initdb.d/init.sql",
            )
        )

    def create_online_store(self) -> Dict[str, Any]:
        self.container.start()
        log_string_to_wait_for = "database system is ready to accept connections"
        wait_for_logs(
            container=self.container, predicate=log_string_to_wait_for, timeout=10
        )
        init_log_string_to_wait_for = "PostgreSQL init process complete"
        wait_for_logs(
            container=self.container, predicate=init_log_string_to_wait_for, timeout=10
        )
        return {
            "host": "localhost",
            "type": "postgres",
            "user": "root",
            "password": "test!@#$%",
            "database": "test",
            "vector_enabled": True,
            "port": self.container.get_exposed_port(5432),
        }

    def teardown(self):
        self.container.stop()
