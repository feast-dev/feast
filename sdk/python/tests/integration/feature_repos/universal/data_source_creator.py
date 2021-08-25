from abc import ABC, abstractmethod
from typing import Dict, Optional

import pandas as pd

from feast.data_source import DataSource
from feast.repo_config import FeastConfigBaseModel


class DataSourceCreator(ABC):
    @abstractmethod
    def create_data_source(
        self,
        df: pd.DataFrame,
        destination: Optional[str] = None,
        suffix: Optional[str] = None,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
    ) -> DataSource:
        ...

    @abstractmethod
    def create_offline_store_config(self) -> FeastConfigBaseModel:
        ...

    @abstractmethod
    def teardown(self):
        ...

    @abstractmethod
    def get_prefixed_table_name(self, table_name: str) -> str:
        ...
