# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from typing import Any, Dict, List, Optional

import requests

from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_rest_client import (
    IcebergRestClient,
)

logger = logging.getLogger(__name__)


class UCClient(IcebergRestClient):
    """Unity Catalog client extending IcebergRestClient with governance APIs.

    Provides UC-specific operations that go beyond the standard Iceberg REST
    Catalog specification:
    - Feature table registration (primary keys, schema, metadata)
    - Lineage recording
    - Table properties management

    Standard Iceberg REST operations (load_table, vend_credentials, etc.)
    are inherited from IcebergRestClient.
    """

    @property
    def _uc_base_url(self) -> str:
        """Base URL for UC-specific REST APIs (non-Iceberg standard)."""
        return self.endpoint

    def register_feature_table(
        self,
        fqn: str,
        primary_keys: List[str],
        columns: List[Dict[str, str]],
        properties: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Register or update a feature table in Unity Catalog.

        Maps Feast FeatureView metadata to UC table properties including
        primary key constraints and feature-store-specific annotations.

        Args:
            fqn: Fully qualified table name (catalog.schema.table).
            primary_keys: Column names that form the primary key (entity join keys).
            columns: List of column definitions [{"name": ..., "type": ...}].
            properties: Additional table properties to set.

        Returns:
            True if registration succeeded, False otherwise.
        """
        url = f"{self._uc_base_url}/tables/{fqn}"
        payload: Dict[str, Any] = {
            "properties": {
                **(properties or {}),
                "feast.primary_keys": ",".join(primary_keys),
                "feast.is_feature_table": "true",
            },
        }

        try:
            response = requests.patch(
                url, json=payload, headers=self.headers, timeout=self.timeout
            )
            if response.status_code == 404:
                logger.info(f"Table {fqn} not found for update, attempting creation")
                return self._create_feature_table(
                    fqn, primary_keys, columns, properties
                )
            response.raise_for_status()
            logger.info(f"Updated UC feature table: {fqn}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to register feature table {fqn}: {e}")
            return False

    def _create_feature_table(
        self,
        fqn: str,
        primary_keys: List[str],
        columns: List[Dict[str, str]],
        properties: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Create a new feature table in UC if it doesn't exist."""
        parts = fqn.split(".")
        if len(parts) != 3:
            logger.error(f"Invalid FQN format: {fqn}. Expected catalog.schema.table")
            return False

        catalog_name, schema_name, table_name = parts
        url = f"{self._uc_base_url}/tables"
        payload: Dict[str, Any] = {
            "name": table_name,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "table_type": "MANAGED",
            "data_source_format": "ICEBERG",
            "columns": [
                {
                    "name": col["name"],
                    "type_name": col.get("type", "STRING"),
                    "position": idx,
                    "nullable": col["name"] not in primary_keys,
                }
                for idx, col in enumerate(columns)
            ],
            "properties": {
                **(properties or {}),
                "feast.primary_keys": ",".join(primary_keys),
                "feast.is_feature_table": "true",
            },
        }

        try:
            response = requests.post(
                url, json=payload, headers=self.headers, timeout=self.timeout
            )
            response.raise_for_status()
            logger.info(f"Created UC feature table: {fqn}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create feature table {fqn}: {e}")
            return False

    def record_lineage(
        self,
        target_fqn: str,
        source_fqns: List[str],
    ) -> bool:
        """Record lineage relationships in Unity Catalog.

        Links source tables to the target feature table for governance tracking.

        Args:
            target_fqn: The downstream feature table FQN.
            source_fqns: List of upstream source table FQNs.

        Returns:
            True if lineage was recorded, False otherwise.
        """
        url = f"{self._uc_base_url}/lineage/table-lineage"
        payload = {
            "table_name": target_fqn,
            "upstream_tables": [{"table_name": src} for src in source_fqns],
        }

        try:
            response = requests.post(
                url, json=payload, headers=self.headers, timeout=self.timeout
            )
            response.raise_for_status()
            logger.info(f"Recorded lineage for {target_fqn}: {source_fqns}")
            return True
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to record lineage for {target_fqn}: {e}")
            return False

    def set_table_properties(
        self,
        fqn: str,
        properties: Dict[str, str],
    ) -> bool:
        """Set arbitrary properties on a UC table.

        Used to sync Feast metadata (project, feature_view name, owner, tags)
        into UC table properties for discoverability.

        Args:
            fqn: Fully qualified table name.
            properties: Key-value properties to set.

        Returns:
            True if properties were set, False otherwise.
        """
        url = f"{self._uc_base_url}/tables/{fqn}"
        payload = {"properties": properties}

        try:
            response = requests.patch(
                url, json=payload, headers=self.headers, timeout=self.timeout
            )
            response.raise_for_status()
            logger.info(f"Set properties on {fqn}: {list(properties.keys())}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to set properties on {fqn}: {e}")
            return False

    def get_feature_tables(self, namespace: str) -> List[Dict[str, Any]]:
        """List tables in a namespace that are marked as feature tables.

        Useful for the import-uc-table CLI to discover existing feature tables.

        Args:
            namespace: The UC schema to scan.

        Returns:
            List of table metadata dicts for tables with feast.is_feature_table=true.
        """
        tables = self.list_tables(namespace)
        feature_tables = []

        for table_name in tables:
            fqn = f"{self.warehouse}.{namespace}.{table_name}"
            url = f"{self._uc_base_url}/tables/{fqn}"
            try:
                response = requests.get(url, headers=self.headers, timeout=self.timeout)
                response.raise_for_status()
                data = response.json()
                props = data.get("properties", {})
                if props.get("feast.is_feature_table") == "true":
                    feature_tables.append(data)
            except requests.exceptions.RequestException:
                continue

        return feature_tables
