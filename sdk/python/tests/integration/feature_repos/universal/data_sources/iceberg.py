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

from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg import (
    IcebergOfflineStoreConfig,
)
from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
    IcebergSource,
)
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
        entity_name: str,
        timestamp_field: str,
        created_timestamp_column: str = None,
        field_mapping: dict = None,
    ) -> IcebergSource:
        table_id = f"test_ns.{destination_name}"

        # Simple schema inference for testing
        # In a real implementation, we'd want more robust mapping
        iceberg_schema = Schema(
            *[self._pandas_to_iceberg_type(col, df[col].dtype) for col in df.columns]
        )

        table = self.catalog.create_table(table_id, schema=iceberg_schema)
        # Convert pandas to arrow and write to iceberg
        import pyarrow as pa

        table.append(pa.Table.from_pandas(df))

        return IcebergSource(
            name=destination_name,
            table_identifier=table_id,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
        )

    def _pandas_to_iceberg_type(self, name, dtype):
        from pyiceberg.types import NestedField

        if "int64" in str(dtype):
            return NestedField(
                field_id=None, name=name, field_type=LongType(), required=False
            )
        if "int32" in str(dtype):
            return NestedField(
                field_id=None, name=name, field_type=IntegerType(), required=False
            )
        if "float64" in str(dtype):
            return NestedField(
                field_id=None, name=name, field_type=DoubleType(), required=False
            )
        if "float32" in str(dtype):
            return NestedField(
                field_id=None, name=name, field_type=FloatType(), required=False
            )
        if "bool" in str(dtype):
            return NestedField(
                field_id=None, name=name, field_type=BooleanType(), required=False
            )
        if "datetime" in str(dtype):
            return NestedField(
                field_id=None, name=name, field_type=TimestampType(), required=False
            )
        return NestedField(
            field_id=None, name=name, field_type=StringType(), required=False
        )

    def create_offline_store_config(self) -> IcebergOfflineStoreConfig:
        return IcebergOfflineStoreConfig(
            catalog_type="sql",
            catalog_name="default",
            uri=self.catalog_uri,
            warehouse=self.warehouse_path,
        )
