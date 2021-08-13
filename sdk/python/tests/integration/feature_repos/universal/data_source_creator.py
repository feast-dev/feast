from abc import ABC, abstractmethod
from datetime import timedelta

import pandas as pd

from feast import driver_test_data
from feast.data_source import DataSource
from feast.repo_config import FeastConfigBaseModel


class DataSourceCreator(ABC):
    @abstractmethod
    def create_data_sources(
        self,
        destination: str,
        df: pd.DataFrame,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
    ) -> DataSource:
        ...

    @abstractmethod
    def create_offline_store_config(self) -> FeastConfigBaseModel:
        ...

    @abstractmethod
    def teardown(self):
        ...

    @abstractmethod
    def get_prefixed_table_name(self, name: str, suffix: str) -> str:
        ...
