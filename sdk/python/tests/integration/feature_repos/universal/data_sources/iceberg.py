import os
import shutil
from typing import Dict, Optional

import pandas as pd
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
)

from feast.feature_logging import LoggingDestination
from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
    IcebergOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
    IcebergSource,
)
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.saved_dataset import SavedDatasetStorage
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class IcebergDataSourceCreator(DataSourceCreator):
    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name, *args, **kwargs)
        self.catalog_uri = f"sqlite:///{project_name}_catalog.db"
        self.warehouse_path = f"{project_name}_warehouse"
        self.catalog = load_catalog(
            "default",
            **{
                "type": "sql",
                "uri": self.catalog_uri,
                "warehouse": self.warehouse_path,
            },
        )
        try:
            self.catalog.create_namespace("test_ns")
        except Exception:
            pass

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column="created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = None,
    ) -> IcebergSource:
        table_id = f"test_ns.{destination_name}"

        # Simple schema inference for testing
        # In a real implementation, we'd want more robust mapping
        iceberg_schema = Schema(
            *[
                self._pandas_to_iceberg_type(i + 1, col, df[col].dtype)
                for i, col in enumerate(df.columns)
            ]
        )

        table = self.catalog.create_table(table_id, schema=iceberg_schema)
        # Convert pandas to arrow and write to iceberg
        # Note: Iceberg requires microsecond precision timestamps, not nanosecond
        import pyarrow as pa

        # Build Arrow schema with microsecond timestamps
        arrow_fields = []
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                arrow_fields.append(pa.field(col, pa.timestamp("us")))
            elif pd.api.types.is_integer_dtype(df[col]):
                arrow_fields.append(pa.field(col, pa.int64()))
            elif pd.api.types.is_float_dtype(df[col]):
                arrow_fields.append(pa.field(col, pa.float64()))
            elif pd.api.types.is_bool_dtype(df[col]):
                arrow_fields.append(pa.field(col, pa.bool_()))
            else:
                arrow_fields.append(pa.field(col, pa.string()))

        arrow_schema = pa.schema(arrow_fields)
        arrow_table = pa.Table.from_pandas(df, schema=arrow_schema)
        table.append(arrow_table)

        return IcebergSource(
            name=destination_name,
            table_identifier=table_id,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
        )

    def _pandas_to_iceberg_type(self, field_id: int, name: str, dtype):
        from pyiceberg.types import NestedField

        if "int64" in str(dtype):
            return NestedField(
                field_id=field_id, name=name, field_type=LongType(), required=False
            )
        if "int32" in str(dtype):
            return NestedField(
                field_id=field_id, name=name, field_type=IntegerType(), required=False
            )
        if "float64" in str(dtype):
            return NestedField(
                field_id=field_id, name=name, field_type=DoubleType(), required=False
            )
        if "float32" in str(dtype):
            return NestedField(
                field_id=field_id, name=name, field_type=FloatType(), required=False
            )
        if "bool" in str(dtype):
            return NestedField(
                field_id=field_id, name=name, field_type=BooleanType(), required=False
            )
        if "datetime" in str(dtype):
            return NestedField(
                field_id=field_id, name=name, field_type=TimestampType(), required=False
            )
        return NestedField(
            field_id=field_id, name=name, field_type=StringType(), required=False
        )

    def create_offline_store_config(self) -> IcebergOfflineStoreConfig:
        return IcebergOfflineStoreConfig(
            catalog_type="sql",
            catalog_name="default",
            uri=self.catalog_uri,
            warehouse=self.warehouse_path,
        )

    def create_saved_dataset_destination(self) -> SavedDatasetStorage:
        """Create a file-based storage destination for saved datasets."""
        return SavedDatasetFileStorage(
            path=os.path.join(self.warehouse_path, "saved_datasets"),
            file_format="parquet",
        )

    def create_logged_features_destination(self) -> LoggingDestination:
        """Create a file-based logging destination."""
        from feast.feature_logging import FileLoggingDestination

        return FileLoggingDestination(
            path=os.path.join(self.warehouse_path, "logged_features")
        )

    def teardown(self):
        """Clean up test resources - catalog DB and warehouse directory."""
        # Remove SQLite catalog file
        catalog_file = f"{self.project_name}_catalog.db"
        if os.path.exists(catalog_file):
            os.remove(catalog_file)
        # Remove warehouse directory
        if os.path.exists(self.warehouse_path):
            shutil.rmtree(self.warehouse_path)
