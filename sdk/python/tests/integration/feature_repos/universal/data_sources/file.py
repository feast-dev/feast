import tempfile
from typing import Any, Dict

import pandas as pd

from feast import FileSource
from feast.data_format import ParquetFormat
from feast.data_source import DataSource
from feast.infra.offline_stores.file import FileOfflineStoreConfig
from feast.repo_config import FeastConfigBaseModel
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)


class FileDataSourceCreator(DataSourceCreator):
    f: Any

    def __init__(self, _: str):
        pass

    def create_data_sources(
        self,
        destination: str,
        df: pd.DataFrame,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
    ) -> DataSource:
        self.f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        df.to_parquet(self.f.name)
        return FileSource(
            file_format=ParquetFormat(),
            path=f"file://{self.f.name}",
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            date_partition_column="",
            field_mapping=field_mapping or {"ts_1": "ts", "id": "driver_id"},
        )

    def get_prefixed_table_name(self, name: str, suffix: str) -> str:
        return f"{name}.{suffix}"

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return FileOfflineStoreConfig()

    def teardown(self):
        self.f.close()
