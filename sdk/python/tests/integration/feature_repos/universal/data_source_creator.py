from abc import ABC, abstractmethod
from typing import Dict, Optional

import pandas as pd

from feast.data_source import DataSource
from feast.feature_logging import LoggingDestination
from feast.repo_config import FeastConfigBaseModel
from feast.saved_dataset import SavedDatasetStorage


class DataSourceCreator(ABC):
    def __init__(self, project_name: str, *args, **kwargs):
        self.project_name = project_name

    @abstractmethod
    def create_data_source(
        self,
        df: pd.DataFrame,
        **kwargs,
    ) -> DataSource:
        """
        Create a data source based on the dataframe. Implementing this method requires the underlying implementation to
        persist the dataframe in offline store, using the destination string as a way to differentiate multiple
        dataframes and data sources.

        Args:
            df: The dataframe to be used to create the data source.
            kwargs: Additional arguments to be passed to the underlying data source.

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

    def create_logged_features_destination(self) -> LoggingDestination:
        raise NotImplementedError

    @abstractmethod
    def teardown(self):
        ...
