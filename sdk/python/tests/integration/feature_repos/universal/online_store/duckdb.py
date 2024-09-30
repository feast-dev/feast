import os
from typing import Any, Dict

from testcontainers.core.container import DockerContainer

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class DuckDBOnlineStoreCreator(OnlineStoreCreator):
    def __init__(self,
                 project_name: str,
                 **kwargs):
        super().__init__(project_name)
        self.db_path = os.path.join(os.getcwd(), f"{project_name}_online_store.db")
        self.container = None

    def create_online_store(self) -> Dict[str, Any]:
        self.container = DockerContainer("duckdb/duckdb:latest")
        self.container.with_bind_mount(self.db_path, "/data/online_store.db")
        self.container.start()

        return {
            "type": "duckdb",
            "path": "/data/online_store.db",
            "vector_enabled": True,
        }

    def teardown(self):
        if self.container:
            self.container.stop()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
