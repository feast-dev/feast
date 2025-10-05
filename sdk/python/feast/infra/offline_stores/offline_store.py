# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import warnings
from abc import ABC
from datetime import datetime
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

import pandas as pd
import pyarrow

from feast import flags_helper
from feast.data_source import DataSource
from feast.dataframe import DataFrameEngine, FeastDataFrame
from feast.dqm.errors import ValidationFailed
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.torch_wrapper import get_torch

if TYPE_CHECKING:
    from feast.saved_dataset import ValidationReference

warnings.simplefilter("once", RuntimeWarning)


class RetrievalMetadata:
    min_event_timestamp: Optional[datetime]
    max_event_timestamp: Optional[datetime]

    # List of feature references
    features: List[str]
    # List of entity keys + ODFV inputs
    keys: List[str]

    def __init__(
        self,
        features: List[str],
        keys: List[str],
        min_event_timestamp: Optional[datetime] = None,
        max_event_timestamp: Optional[datetime] = None,
    ):
        self.features = features
        self.keys = keys
        self.min_event_timestamp = min_event_timestamp
        self.max_event_timestamp = max_event_timestamp


class RetrievalJob(ABC):
    """A RetrievalJob manages the execution of a query to retrieve data from the offline store."""

    def to_df(
        self,
        validation_reference: Optional["ValidationReference"] = None,
        timeout: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Synchronously executes the underlying query and returns the result as a pandas dataframe.

        On demand transformations will be executed. If a validation reference is provided, the dataframe
        will be validated.

        Args:
            validation_reference (optional): The validation to apply against the retrieved dataframe.
            timeout (optional): The query timeout if applicable.
        """
        return (
            self.to_arrow(validation_reference=validation_reference, timeout=timeout)
            .to_pandas()
            .reset_index(drop=True)
        )

    def to_feast_df(
        self,
        validation_reference: Optional["ValidationReference"] = None,
        timeout: Optional[int] = None,
    ) -> FeastDataFrame:
        """
        Synchronously executes the underlying query and returns the result as a FeastDataFrame.

        This is the new primary method that returns FeastDataFrame with proper engine detection.
        On demand transformations will be executed. If a validation reference is provided, the dataframe
        will be validated.

        Args:
            validation_reference (optional): The validation to apply against the retrieved dataframe.
            timeout (optional): The query timeout if applicable.
        """
        # Get Arrow table as before
        arrow_table = self.to_arrow(
            validation_reference=validation_reference, timeout=timeout
        )

        # Prepare metadata
        metadata = {}

        # Add features to metadata if available
        if hasattr(self, "features"):
            metadata["features"] = self.features
        else:
            metadata["features"] = []

        # Add on-demand feature views to metadata
        if hasattr(self, "on_demand_feature_views") and self.on_demand_feature_views:
            metadata["on_demand_feature_views"] = [
                odfv.name for odfv in self.on_demand_feature_views
            ]
        else:
            metadata["on_demand_feature_views"] = []

        # Wrap in FeastDataFrame with Arrow engine and metadata
        return FeastDataFrame(
            data=arrow_table, engine=DataFrameEngine.ARROW, metadata=metadata
        )

    def to_arrow(
        self,
        validation_reference: Optional["ValidationReference"] = None,
        timeout: Optional[int] = None,
    ) -> pyarrow.Table:
        """
        Synchronously executes the underlying query and returns the result as an arrow table.

        On demand transformations will be executed. If a validation reference is provided, the dataframe
        will be validated.

        Args:
            validation_reference (optional): The validation to apply against the retrieved dataframe.
            timeout (optional): The query timeout if applicable.
        """
        features_table = self._to_arrow_internal(timeout=timeout)
        if self.on_demand_feature_views:
            for odfv in self.on_demand_feature_views:
                transformed_arrow = odfv.transform_arrow(
                    features_table, self.full_feature_names
                )

                for col in transformed_arrow.column_names:
                    if col.startswith("__index"):
                        continue
                    features_table = features_table.append_column(
                        col, transformed_arrow[col]
                    )

        if validation_reference:
            if not flags_helper.is_test():
                warnings.warn(
                    "Dataset validation is an experimental feature. "
                    "This API is unstable and it could and most probably will be changed in the future. "
                    "We do not guarantee that future changes will maintain backward compatibility.",
                    RuntimeWarning,
                )

            validation_result = validation_reference.profile.validate(
                features_table.to_pandas()
            )
            if not validation_result.is_success:
                raise ValidationFailed(validation_result)

        return features_table

    def to_tensor(
        self,
        kind: str = "torch",
        default_value: Any = float("nan"),
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Converts historical features into a dictionary of 1D torch tensors or lists (for non-numeric types).

        Args:
            kind: "torch" (default and only supported kind).
            default_value: Value to replace missing (None or NaN) entries.
            timeout: Optional timeout for query execution.

        Returns:
            Dict[str, Union[torch.Tensor, List]]: Feature column name -> tensor or list.
        """
        if kind != "torch":
            raise ValueError(
                f"Unsupported tensor kind: {kind}. Only 'torch' is supported."
            )
        torch = get_torch()
        device = "cuda" if torch.cuda.is_available() else "cpu"
        df = self.to_df(timeout=timeout)
        tensor_dict = {}
        for column in df.columns:
            values = df[column].fillna(default_value).tolist()
            first_non_null = next((v for v in values if v is not None), None)
            if isinstance(first_non_null, (int, float, bool)):
                tensor_dict[column] = torch.tensor(values, device=device)
            else:
                tensor_dict[column] = values
        return tensor_dict

    def to_sql(self) -> str:
        """
        Return RetrievalJob generated SQL statement if applicable.
        """
        raise NotImplementedError

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        """
        Synchronously executes the underlying query and returns the result as a pandas dataframe.

        timeout: RetreivalJob implementations may implement a timeout.

        Does not handle on demand transformations or dataset validation. For either of those,
        `to_df` should be used.
        """
        raise NotImplementedError

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        """
        Synchronously executes the underlying query and returns the result as an arrow table.

        timeout: RetreivalJob implementations may implement a timeout.

        Does not handle on demand transformations or dataset validation. For either of those,
        `to_arrow` should be used.
        """
        raise NotImplementedError

    @property
    def full_feature_names(self) -> bool:
        """Returns True if full feature names should be applied to the results of the query."""
        raise NotImplementedError

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        """Returns a list containing all the on demand feature views to be handled."""
        raise NotImplementedError

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: bool = False,
        timeout: Optional[int] = None,
    ):
        """
        Synchronously executes the underlying query and persists the result in the same offline store
        at the specified destination.

        Args:
            storage: The saved dataset storage object specifying where the result should be persisted.
            allow_overwrite: If True, a pre-existing location (e.g. table or file) can be overwritten.
                Currently not all individual offline store implementations make use of this parameter.
        """
        raise NotImplementedError

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        """Returns metadata about the retrieval job."""
        raise NotImplementedError

    def supports_remote_storage_export(self) -> bool:
        """Returns True if the RetrievalJob supports `to_remote_storage`."""
        return False

    def to_remote_storage(self) -> List[str]:
        """
        Synchronously executes the underlying query and exports the results to remote storage (e.g. S3 or GCS).

        Implementations of this method should export the results as multiple parquet files, each file sized
        appropriately depending on how much data is being returned by the retrieval job.

        Returns:
            A list of parquet file paths in remote storage.
        """
        raise NotImplementedError


class OfflineStore(ABC):
    """
    An offline store defines the interface that Feast uses to interact with the storage and compute system that
    handles offline features.

    Each offline store implementation is designed to work only with the corresponding data source. For example,
    the SnowflakeOfflineStore can handle SnowflakeSources but not FileSources.
    """

    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        """
        Extracts the latest entity rows (i.e. the combination of join key columns, feature columns, and
        timestamp columns) from the specified data source that lie within the specified time range.

        All of the column names should refer to columns that exist in the data source. In particular,
        any mapping of column names must have already happened.

        Args:
            config: The config for the current feature store.
            data_source: The data source from which the entity rows will be extracted.
            join_key_columns: The columns of the join keys.
            feature_name_columns: The columns of the features.
            timestamp_field: The timestamp column, used to determine which rows are the most recent.
            created_timestamp_column: The column indicating when the row was created, used to break ties.
            start_date: The start of the time range.
            end_date: The end of the time range.

        Returns:
            A RetrievalJob that can be executed to get the entity rows.
        """
        raise NotImplementedError

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Optional[Union[pd.DataFrame, str]],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        """
        Retrieves the point-in-time correct historical feature values for the specified entity rows.

        Args:
            config: The config for the current feature store.
            feature_views: A list containing all feature views that are referenced in the entity rows.
            feature_refs: The features to be retrieved.
            entity_df: A collection of rows containing all entity columns on which features need to be joined,
                as well as the timestamp column used for point-in-time joins. Either a pandas dataframe can be
                provided or a SQL query. If None, features will be retrieved for the specified timestamp range.
            registry: The registry for the current feature store.
            project: Feast project to which the feature views belong.
            full_feature_names: If True, feature names will be prefixed with the corresponding feature view name,
                changing them from the format "feature" to "feature_view__feature" (e.g. "daily_transactions"
                changes to "customer_fv__daily_transactions").

        Keyword Args:
            start_date: Start date for the timestamp range when retrieving features without entity_df.
            end_date: End date for the timestamp range when retrieving features without entity_df. By default, the current time is used.

        Returns:
            A RetrievalJob that can be executed to get the features.
        """
        raise NotImplementedError

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> RetrievalJob:
        """
        Extracts all the entity rows (i.e. the combination of join key columns, feature columns, and
        timestamp columns) from the specified data source that lie within the specified time range.

        All of the column names should refer to columns that exist in the data source. In particular,
        any mapping of column names must have already happened.

        Args:
            config: The config for the current feature store.
            data_source: The data source from which the entity rows will be extracted.
            join_key_columns: The columns of the join keys.
            feature_name_columns: The columns of the features.
            timestamp_field: The timestamp column, used to determine which rows are the most recent.
            created_timestamp_column (Optional): The column indicating when the row was created, used to break ties.
            start_date (Optional): The start of the time range.
            end_date (Optional): The end of the time range.

        Returns:
            A RetrievalJob that can be executed to get the entity rows.
        """
        raise NotImplementedError

    @staticmethod
    def write_logged_features(
        config: RepoConfig,
        data: Union[pyarrow.Table, Path],
        source: LoggingSource,
        logging_config: LoggingConfig,
        registry: BaseRegistry,
    ):
        """
        Writes logged features to a specified destination in the offline store.

        If the specified destination exists, data will be appended; otherwise, the destination will be
        created and data will be added. Thus this function can be called repeatedly with the same
        destination to flush logs in chunks.

        Args:
            config: The config for the current feature store.
            data: An arrow table or a path to parquet directory that contains the logs to write.
            source: The logging source that provides a schema and some additional metadata.
            logging_config: A LoggingConfig object that determines where the logs will be written.
            registry: The registry for the current feature store.
        """
        raise NotImplementedError

    @staticmethod
    def offline_write_batch(
        config: RepoConfig,
        feature_view: FeatureView,
        table: pyarrow.Table,
        progress: Optional[Callable[[int], Any]],
    ):
        """
        Writes the specified arrow table to the data source underlying the specified feature view.

        Args:
            config: The config for the current feature store.
            feature_view: The feature view whose batch source should be written.
            table: The arrow table to write.
            progress: Function to be called once a portion of the data has been written, used
                to show progress.
        """
        raise NotImplementedError

    def validate_data_source(
        self,
        config: RepoConfig,
        data_source: DataSource,
    ):
        """
        Validates the underlying data source.

        Args:
            config: Configuration object used to configure a feature store.
            data_source: DataSource object that needs to be validated
        """
        data_source.validate(config=config)

    def get_table_column_names_and_types_from_data_source(
        self,
        config: RepoConfig,
        data_source: DataSource,
    ) -> Iterable[Tuple[str, str]]:
        """
        Returns the list of column names and raw column types for a DataSource.

        Args:
            config: Configuration object used to configure a feature store.
            data_source: DataSource object
        """
        return data_source.get_table_column_names_and_types(config=config)
