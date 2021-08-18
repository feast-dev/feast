import tempfile
from typing import Any, Dict, List

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
    files: List[Any]

    def __init__(self, project_name: str):
        self.project_name = project_name
        self.files = []

    def create_data_sources(
        self,
        destination: str,
        df: pd.DataFrame,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
    ) -> DataSource:
        f = tempfile.NamedTemporaryFile(
            prefix=self.project_name, suffix=".parquet", delete=False
        )
        df.to_parquet(f.name)
        self.files.append(f)
        return FileSource(
            file_format=ParquetFormat(),
            path=f"file://{f.name}",
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            date_partition_column="",
            field_mapping=field_mapping or {"ts_1": "ts"},
        )

    def get_prefixed_table_name(self, suffix: str) -> str:
        return f"{self.project_name}.{suffix}"

    def create_offline_store_config(self) -> FeastConfigBaseModel:
        return FileOfflineStoreConfig()

    def teardown(self):
        for f in self.files:
            f.close()
