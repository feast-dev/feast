import os
import shutil
from typing import Any, Dict

from pyiceberg.catalog import load_catalog

from tests.integration.feature_repos.universal.online_store_creator import (
    OnlineStoreCreator,
)


class IcebergOnlineStoreCreator(OnlineStoreCreator):
    """
    Creator for Iceberg online store using local SQLite catalog.

    This is for integration testing only - uses local filesystem storage.
    """

    def __init__(self, project_name: str, **kwargs):
        super().__init__(project_name)
        self.catalog_uri = f"sqlite:///{project_name}_online_catalog.db"
        self.warehouse_path = f"{project_name}_online_warehouse"

        # Create catalog
        self.catalog = load_catalog(
            "online_catalog",
            **{
                "type": "sql",
                "uri": self.catalog_uri,
                "warehouse": self.warehouse_path,
            },
        )

        # Create namespace for online store tables
        try:
            self.catalog.create_namespace("online")
        except Exception:
            # Namespace might already exist
            pass

    def create_online_store(self) -> Dict[str, Any]:
        """Return configuration for Iceberg online store."""
        return {
            "type": "iceberg",
            "catalog_type": "sql",
            "catalog_name": "online_catalog",
            "uri": self.catalog_uri,
            "warehouse": self.warehouse_path,
            "namespace": "online",
            "partition_strategy": "entity_hash",  # Default strategy for tests
        }

    def teardown(self):
        """Clean up test resources - catalog DB and warehouse directory."""
        # Remove SQLite catalog file
        catalog_file = f"{self.project_name}_online_catalog.db"
        if os.path.exists(catalog_file):
            os.remove(catalog_file)

        # Remove warehouse directory
        if os.path.exists(self.warehouse_path):
            shutil.rmtree(self.warehouse_path)
