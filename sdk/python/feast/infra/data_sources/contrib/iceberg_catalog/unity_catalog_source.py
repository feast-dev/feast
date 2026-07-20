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

import json
import os
from typing import Any, Dict, Optional

from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import (
    IcebergSource,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto


class UnityCatalogSource(IcebergSource):
    """Data source backed by Databricks Unity Catalog.

    Extends IcebergSource with UC-specific governance capabilities:
    - Feature table registration on ``feast apply``
    - Lineage sync between Feast feature views and UC lineage graph
    - UC table properties sync (feast.project, feast.feature_view, etc.)

    Connection defaults to DATABRICKS_HOST and DATABRICKS_TOKEN env vars.
    """

    def __init__(
        self,
        *,
        warehouse: str,
        namespace: str,
        table: str,
        name: Optional[str] = None,
        timestamp_field: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        endpoint: Optional[str] = None,
        token_env_var: Optional[str] = None,
        credential_vending: bool = True,
        catalog_type: str = "rest",
        catalog_name: str = "feast_iceberg",
        catalog_properties: Optional[Dict[str, str]] = None,
        register_as_feature_table: bool = True,
        sync_lineage: bool = True,
    ):
        if endpoint is None:
            host = os.environ.get("DATABRICKS_HOST", "")
            endpoint = f"{host}/api/2.1/unity-catalog/iceberg"

        if token_env_var is None:
            token_env_var = "DATABRICKS_TOKEN"

        super().__init__(
            endpoint=endpoint,
            warehouse=warehouse,
            namespace=namespace,
            table=table,
            catalog_type=catalog_type,
            catalog_name=catalog_name,
            catalog_properties=catalog_properties,
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
            token_env_var=token_env_var,
            credential_vending=credential_vending,
        )
        self.register_as_feature_table = register_as_feature_table
        self.sync_lineage = sync_lineage

    def get_uc_client(self):
        """Create a UCClient for governance operations beyond standard Iceberg REST."""
        from feast.infra.data_sources.contrib.iceberg_catalog.uc_client import (
            UCClient,
        )

        uc_endpoint = self.endpoint
        iceberg_suffix = "/api/2.1/unity-catalog/iceberg"
        if uc_endpoint.endswith(iceberg_suffix):
            uc_endpoint = uc_endpoint[: -len("/iceberg")]
        elif uc_endpoint.endswith("/iceberg"):
            uc_endpoint = uc_endpoint[: -len("/iceberg")]

        token = os.environ.get(self.token_env_var) if self.token_env_var else None
        return UCClient(
            endpoint=uc_endpoint,
            token=token,
            warehouse=self.warehouse,
        )

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> Any:
        custom_options = json.loads(
            str(data_source.custom_options.configuration, encoding="utf8")
        )
        return UnityCatalogSource(
            name=data_source.name,
            endpoint=custom_options["endpoint"],
            warehouse=custom_options["warehouse"],
            namespace=custom_options["namespace"],
            table=custom_options["table"],
            token_env_var=custom_options.get("token_env_var"),
            credential_vending=custom_options.get("credential_vending", True),
            catalog_type=custom_options.get("catalog_type", "rest"),
            catalog_name=custom_options.get("catalog_name", "feast_iceberg"),
            catalog_properties=custom_options.get("catalog_properties"),
            register_as_feature_table=custom_options.get(
                "register_as_feature_table", True
            ),
            sync_lineage=custom_options.get("sync_lineage", True),
            timestamp_field=data_source.timestamp_field,
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=dict(data_source.field_mapping),
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        config_json = json.dumps(
            {
                "endpoint": self.endpoint,
                "warehouse": self.warehouse,
                "namespace": self.namespace,
                "table": self.iceberg_table,
                "token_env_var": self.token_env_var,
                "credential_vending": self.credential_vending,
                "catalog_type": self.catalog_type,
                "catalog_name": self.catalog_name,
                "catalog_properties": self.catalog_properties,
                "register_as_feature_table": self.register_as_feature_table,
                "sync_lineage": self.sync_lineage,
            }
        )
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.BATCH_ICEBERG,
            data_source_class_type=(
                "feast.infra.data_sources.contrib.iceberg_catalog"
                ".unity_catalog_source.UnityCatalogSource"
            ),
            custom_options=DataSourceProto.CustomSourceOptions(
                configuration=bytes(config_json, encoding="utf8")
            ),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            timestamp_field=self.timestamp_field,
            created_timestamp_column=self.created_timestamp_column,
            field_mapping=self.field_mapping,
        )
        return data_source_proto

    def __eq__(self, other):
        if not isinstance(other, UnityCatalogSource):
            return False
        if not super().__eq__(other):
            return False
        return (
            self.register_as_feature_table == other.register_as_feature_table
            and self.sync_lineage == other.sync_lineage
        )

    def __hash__(self):
        return hash(
            (
                self.name,
                self.endpoint,
                self.warehouse,
                self.namespace,
                self.iceberg_table,
            )
        )
