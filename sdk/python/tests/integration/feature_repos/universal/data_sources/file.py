import tempfile
from typing import Any

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

    def create_data_source(
        self,
        name: str,
        df: pd.DataFrame,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
    ) -> DataSource:
        self.f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        df.to_parquet(self.f.name)
        return FileSource(
            file_format=ParquetFormat(),
            path=f"file://{self.f.name}",
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            date_partition_column="",
            field_mapping={"ts_1": "ts", "id": "driver_id"},
        )

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return FileOfflineStoreConfig()

    def teardown(self, name: str):
        self.f.close()
