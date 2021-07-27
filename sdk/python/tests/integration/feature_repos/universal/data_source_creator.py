from abc import ABC, abstractmethod

import pandas as pd

from feast.data_source import DataSource


class DataSourceCreator(ABC):
    @abstractmethod
    def create_data_source(
        self,
        name: str,
        df: pd.DataFrame,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        **kwargs,
    ) -> DataSource:
        ...

    @abstractmethod
    def create_offline_store_config(self):
        pass

    @abstractmethod
    def teardown(self, name: str):
        ...
