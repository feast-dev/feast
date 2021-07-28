from abc import ABC, abstractmethod

import pandas as pd

from feast.data_source import DataSource
from feast.repo_config import FeastConfigBaseModel


class DataSourceCreator(ABC):
    @abstractmethod
    def create_data_source(
        self,
        name: str,
        df: pd.DataFrame,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
    ) -> DataSource:
        ...

    @abstractmethod
    def create_offline_store_config(self) -> FeastConfigBaseModel:
        ...

    @abstractmethod
    def teardown(self, name: str):
        ...
