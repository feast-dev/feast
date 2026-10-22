"""DataSourceCreator for IcebergSource with DuckDB offline store.

Used by the universal integration test suite to parametrize tests against
IcebergSource backed by the DuckDB offline store.
"""

import os
import tempfile
from typing import Dict, Optional

import pandas as pd

from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.infra.data_sources.contrib.iceberg_catalog.iceberg_source import (
    IcebergSource,
)
from feast.infra.offline_stores.duckdb import DuckDBOfflineStoreConfig
from feast.repo_config import FeastConfigBaseModel
from tests.universal.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class IcebergDataSourceCreator(DataSourceCreator):
    """Creates IcebergSource instances backed by local Parquet files.

    For testing without a real Iceberg REST Catalog server, this creator writes
    DataFrames to local Parquet files. The DuckDB offline store's fallback path
    reads Parquet directly from the resolved location via file:// endpoint.
    """

    tables: Dict[str, str]

    def __init__(self, project_name: str, *args, **kwargs):
        super().__init__(project_name)
        self.tables = {}
        self._temp_dir = tempfile.mkdtemp(prefix="feast_iceberg_test_")

    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column: Optional[str] = "created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = "ts",
    ) -> DataSource:
        """Write df to a local Parquet file, return an IcebergSource."""
        table_dir = os.path.join(self._temp_dir, destination_name)
        os.makedirs(table_dir, exist_ok=True)
        parquet_path = os.path.join(table_dir, "data.parquet")
        df.to_parquet(parquet_path, index=False)
        self.tables[destination_name] = table_dir

        return IcebergSource(
            endpoint="file://local",
            warehouse="test",
            namespace="default",
            table=destination_name,
            name=destination_name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping or {},
            token_env_var=None,
            credential_vending=False,
        )

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return DuckDBOfflineStoreConfig()

    def create_saved_dataset_destination(self):
        from feast.infra.offline_stores.file_source import SavedDatasetFileStorage

        return SavedDatasetFileStorage(
            path=os.path.join(self._temp_dir, "saved_datasets"),
            file_format="parquet",
        )

    def create_logged_features_destination(self) -> LoggingDestination:
        from feast.infra.offline_stores.file_source import FileLoggingDestination

        return FileLoggingDestination(
            path=os.path.join(self._temp_dir, "logged_features"),
        )

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}_{suffix}"

    def teardown(self):
        import shutil

        shutil.rmtree(self._temp_dir, ignore_errors=True)
