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
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from feast.data_source import DataSource
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
from feast.value_type import ValueType


class IcebergSource(DataSource):
    """Data source backed by Apache Iceberg tables via any supported catalog.

    Supports multiple catalog backends through the ``catalog_type`` parameter:
    - ``"rest"`` (default): Iceberg REST Catalog protocol (Unity Catalog, Polaris, Nessie, etc.)
    - ``"hive"``: Hive Metastore catalog
    - ``"glue"``: AWS Glue catalog
    - ``"sql"``: SQL-based catalog (JDBC)
    - ``"dynamodb"``: DynamoDB-based catalog

    Additional catalog-specific configuration can be passed via ``catalog_properties``.
    """

    def __init__(
        self,
        *,
        warehouse: str,
        namespace: str,
        table: str,
        catalog_type: str = "rest",
        catalog_name: str = "feast_iceberg",
        endpoint: Optional[str] = None,
        catalog_properties: Optional[Dict[str, str]] = None,
        name: Optional[str] = None,
        timestamp_field: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        token_env_var: Optional[str] = None,
        credential_vending: bool = True,
    ):
        _name = name or f"{warehouse}.{namespace}.{table}"
        super().__init__(
            name=_name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )
        self.catalog_type = catalog_type
        self.catalog_name = catalog_name
        self.endpoint = endpoint or ""
        self.warehouse = warehouse
        self.namespace = namespace
        self.iceberg_table = table
        self.token_env_var = token_env_var
        self.credential_vending = credential_vending
        self.catalog_properties = catalog_properties or {}

    @property
    def fqn(self) -> str:
        """Fully qualified table name: warehouse.namespace.table."""
        return f"{self.warehouse}.{self.namespace}.{self.iceberg_table}"

    def get_catalog_client(self):
        """Create a catalog client based on catalog_type.

        For "rest": returns an IcebergRestClient (built-in lightweight client).
        For other types: delegates to PyIceberg's load_catalog().
        """
        if self.catalog_type == "rest":
            from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_rest_client import (
                IcebergRestClient,
            )

            token = None
            if self.token_env_var:
                token = os.environ.get(self.token_env_var)
                if not token:
                    import logging

                    logging.getLogger(__name__).warning(
                        "token_env_var '%s' is set but the environment "
                        "variable is empty or missing",
                        self.token_env_var,
                    )
            return IcebergRestClient(
                endpoint=self.endpoint,
                token=token,
                warehouse=self.warehouse,
                credential_vending=self.credential_vending,
            )

        from pyiceberg.catalog import load_catalog

        config = {
            "type": self.catalog_type,
            **self.catalog_properties,
        }
        if self.endpoint:
            config.setdefault("uri", self.endpoint)
        if self.warehouse:
            config.setdefault("warehouse", self.warehouse)
        if self.token_env_var:
            token = os.environ.get(self.token_env_var, "")
            if token:
                config.setdefault("token", token)
        return load_catalog(self.catalog_name, **config)

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.BATCH_ICEBERG

    @staticmethod
    def from_proto(data_source: DataSourceProto) -> Any:
        cls_type = data_source.data_source_class_type
        if cls_type and "UnityCatalogSource" in cls_type:
            from feast.infra.data_sources.contrib.iceberg_catalog.unity_catalog_source import (
                UnityCatalogSource,
            )

            return UnityCatalogSource.from_proto(data_source)

        custom_options = json.loads(
            str(data_source.custom_options.configuration, encoding="utf8")
        )
        return IcebergSource(
            name=data_source.name,
            catalog_type=custom_options.get("catalog_type", "rest"),
            catalog_name=custom_options.get("catalog_name", "feast_iceberg"),
            endpoint=custom_options.get("endpoint", ""),
            warehouse=custom_options["warehouse"],
            namespace=custom_options["namespace"],
            table=custom_options["table"],
            token_env_var=custom_options.get("token_env_var"),
            credential_vending=custom_options.get("credential_vending", True),
            catalog_properties=custom_options.get("catalog_properties", {}),
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
                "catalog_type": self.catalog_type,
                "catalog_name": self.catalog_name,
                "endpoint": self.endpoint,
                "warehouse": self.warehouse,
                "namespace": self.namespace,
                "table": self.iceberg_table,
                "token_env_var": self.token_env_var,
                "credential_vending": self.credential_vending,
                "catalog_properties": self.catalog_properties,
            }
        )
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.BATCH_ICEBERG,
            data_source_class_type=(
                "feast.infra.data_sources.contrib.iceberg_catalog"
                ".iceberg_source.IcebergSource"
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

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return _iceberg_type_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """Retrieve column names and types from the Iceberg catalog."""
        client = self.get_catalog_client()
        if self.catalog_type == "rest":
            metadata = client.load_table(self.namespace, self.iceberg_table)
            if metadata.schema_fields:
                return [(f["name"], f["type"]) for f in metadata.schema_fields]
            return []
        else:
            table = client.load_table(f"{self.namespace}.{self.iceberg_table}")
            return [
                (field.name, str(field.field_type)) for field in table.schema().fields
            ]

    def get_table_query_string(self) -> str:
        """Return a SQL-friendly reference for this source (temp view name)."""
        return f"iceberg_tmp_{self.iceberg_table}"

    def validate(self, config: RepoConfig):
        """Validates connectivity to the catalog.

        Raises:
            ValueError: If the table cannot be loaded.
        """
        from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_rest_client import (
            CatalogAuthError,
            IcebergCatalogError,
            TableNotFoundError,
        )

        try:
            client = self.get_catalog_client()
            if self.catalog_type == "rest":
                client.load_table(self.namespace, self.iceberg_table)
            else:
                client.load_table(f"{self.namespace}.{self.iceberg_table}")
        except TableNotFoundError:
            raise ValueError(
                f"Table '{self.fqn}' not found in catalog "
                f"(type={self.catalog_type}) at '{self.endpoint}'"
            )
        except CatalogAuthError as e:
            raise ValueError(
                f"Authentication failed for catalog at '{self.endpoint}': {e}"
            )
        except IcebergCatalogError as e:
            raise ValueError(
                f"Could not load table '{self.fqn}' from catalog "
                f"(type={self.catalog_type}) at '{self.endpoint}': {e}"
            )
        except Exception as e:
            raise ValueError(
                f"Could not load table '{self.fqn}' from catalog "
                f"(type={self.catalog_type}) at '{self.endpoint}': {e}"
            )

    def __eq__(self, other):
        if not isinstance(other, IcebergSource):
            return False
        if not super().__eq__(other):
            return False
        return (
            self.catalog_type == other.catalog_type
            and self.catalog_name == other.catalog_name
            and self.endpoint == other.endpoint
            and self.warehouse == other.warehouse
            and self.namespace == other.namespace
            and self.iceberg_table == other.iceberg_table
            and self.token_env_var == other.token_env_var
            and self.credential_vending == other.credential_vending
            and self.catalog_properties == other.catalog_properties
        )

    def __hash__(self):
        return hash(
            (
                self.name,
                self.catalog_type,
                self.endpoint,
                self.warehouse,
                self.namespace,
                self.iceberg_table,
            )
        )


def _iceberg_type_to_feast_value_type(iceberg_type: str) -> ValueType:
    """Maps Iceberg data types to Feast ValueTypes."""
    type_map = {
        "boolean": ValueType.BOOL,
        "int": ValueType.INT32,
        "long": ValueType.INT64,
        "float": ValueType.FLOAT,
        "double": ValueType.DOUBLE,
        "string": ValueType.STRING,
        "binary": ValueType.BYTES,
        "date": ValueType.INT32,
        "timestamp": ValueType.INT64,
        "timestamptz": ValueType.INT64,
        "decimal": ValueType.DOUBLE,
        "uuid": ValueType.STRING,
        "fixed": ValueType.BYTES,
    }
    normalized = iceberg_type.lower().split("(")[0].strip()
    return type_map.get(normalized, ValueType.UNKNOWN)
