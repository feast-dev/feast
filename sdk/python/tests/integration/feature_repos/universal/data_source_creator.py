from abc import ABC, abstractmethod
from typing import Dict, Optional

import pandas as pd
from _pytest.mark import MarkDecorator

from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.repo_config import FeastConfigBaseModel, RegistryConfig
from feast.saved_dataset import SavedDatasetStorage


class DataSourceCreator(ABC):
    def __init__(self, project_name: str, *args, **kwargs):
        self.project_name = project_name

    @abstractmethod
    def create_data_source(
        self,
        df: pd.DataFrame,
        destination_name: str,
        created_timestamp_column="created_ts",
        field_mapping: Optional[Dict[str, str]] = None,
        timestamp_field: Optional[str] = None,
    ) -> DataSource:
        """
        Create a data source based on the dataframe. Implementing this method requires the underlying implementation to
        persist the dataframe in offline store, using the destination string as a way to differentiate multiple
        dataframes and data sources.

        Args:
            df: The dataframe to be used to create the data source.
            destination_name: This str is used by the implementing classes to
                isolate the multiple dataframes from each other.
            created_timestamp_column: Pass through for the underlying data source.
            field_mapping: Pass through for the underlying data source.
            timestamp_field: Pass through for the underlying data source.


        Returns:
            A Data source object, pointing to a table or file that is uploaded/persisted for the purpose of the
            test.
        """
        raise NotImplementedError

    def setup(self, registry: RegistryConfig):
        pass

    @abstractmethod
    def create_offline_store_config(self) -> FeastConfigBaseModel:
        raise NotImplementedError

    @abstractmethod
    def create_saved_dataset_destination(self) -> SavedDatasetStorage:
        raise NotImplementedError

    @abstractmethod
    def create_logged_features_destination(self) -> LoggingDestination:
        raise NotImplementedError

    @abstractmethod
    def teardown(self):
        raise NotImplementedError

    @staticmethod
    def xdist_groups() -> list[str]:
        return []

    @staticmethod
    def test_markers() -> list[MarkDecorator]:
        """
        return the array of test markers to add dynamically to the tests created by this creator method. override this method in your implementations. By default, it will not add any markers.
        :return:
        """
        return []
