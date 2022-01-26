from abc import ABC, abstractmethod
from typing import Dict

import pandas as pd

from feast.data_source import DataSource
from feast.repo_config import FeastConfigBaseModel
from feast.saved_dataset import SavedDatasetStorage


class DataSourceCreator(ABC):
    @abstractmethod
    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        event_timestamp_column="ts",
        created_timestamp_column="created_ts",
        field_mapping: Dict[str, str] = None,
    ) -> DataSource:
        """
        Create a data source based on the dataframe. Implementing this method requires the underlying implementation to
        persist the dataframe in offline store, using the destination string as a way to differentiate multiple
        dataframes and data sources.

        Args:
            df: The dataframe to be used to create the data source.
            destination_name: This str is used by the implementing classes to
                isolate the multiple dataframes from each other.
            event_timestamp_column: Pass through for the underlying data source.
            created_timestamp_column: Pass through for the underlying data source.
            field_mapping: Pass through for the underlying data source.

        Returns:
            A Data source object, pointing to a table or file that is uploaded/persisted for the purpose of the
            test.
        """
        ...

    @abstractmethod
    def create_offline_store_config(self) -> FeastConfigBaseModel:
        ...

    @abstractmethod
    def create_saved_dataset_destination(self) -> SavedDatasetStorage:
        ...

    @abstractmethod
    def teardown(self):
        ...
