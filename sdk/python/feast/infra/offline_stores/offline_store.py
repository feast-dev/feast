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
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Union

import pandas as pd
import pyarrow

from feast import flags_helper
from feast.data_source import DataSource
from feast.dqm.errors import ValidationFailed
from feast.feature_logging import LoggingConfig, LoggingSource
from feast.feature_view import FeatureView
from feast.infra.registry.base_registry import BaseRegistry
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage

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
        features_df = self._to_df_internal(timeout=timeout)

        if self.on_demand_feature_views:
            # TODO(adchia): Fix requirement to specify dependent feature views in feature_refs
            for odfv in self.on_demand_feature_views:
                features_df = features_df.join(
                    odfv.get_transformed_features_df(
                        features_df,
                        self.full_feature_names,
                    )
                )

        if validation_reference:
            if not flags_helper.is_test():
                warnings.warn(
                    "Dataset validation is an experimental feature. "
                    "This API is unstable and it could and most probably will be changed in the future. "
                    "We do not guarantee that future changes will maintain backward compatibility.",
                    RuntimeWarning,
                )

            validation_result = validation_reference.profile.validate(features_df)
            if not validation_result.is_success:
                raise ValidationFailed(validation_result)

        return features_df

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
        if not self.on_demand_feature_views and not validation_reference:
            return self._to_arrow_internal(timeout=timeout)

        features_df = self._to_df_internal(timeout=timeout)
        if self.on_demand_feature_views:
            for odfv in self.on_demand_feature_views:
                features_df = features_df.join(
                    odfv.get_transformed_features_df(
                        features_df,
                        self.full_feature_names,
                    )
                )

        if validation_reference:
            if not flags_helper.is_test():
                warnings.warn(
                    "Dataset validation is an experimental feature. "
                    "This API is unstable and it could and most probably will be changed in the future. "
                    "We do not guarantee that future changes will maintain backward compatibility.",
                    RuntimeWarning,
                )

            validation_result = validation_reference.profile.validate(features_df)
            if not validation_result.is_success:
                raise ValidationFailed(validation_result)

        return pyarrow.Table.from_pandas(features_df)

    def to_sql(self) -> str:
        """
        Return RetrievalJob generated SQL statement if applicable.
        """
        pass

    @abstractmethod
    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        """
        Synchronously executes the underlying query and returns the result as a pandas dataframe.

        timeout: RetreivalJob implementations may implement a timeout.

        Does not handle on demand transformations or dataset validation. For either of those,
        `to_df` should be used.
        """
        pass

    @abstractmethod
    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pyarrow.Table:
        """
        Synchronously executes the underlying query and returns the result as an arrow table.

        timeout: RetreivalJob implementations may implement a timeout.

        Does not handle on demand transformations or dataset validation. For either of those,
        `to_arrow` should be used.
        """
        pass

    @property
    @abstractmethod
    def full_feature_names(self) -> bool:
        """Returns True if full feature names should be applied to the results of the query."""
        pass

    @property
    @abstractmethod
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        """Returns a list containing all the on demand feature views to be handled."""
        pass

    @abstractmethod
    def persist(self, storage: SavedDatasetStorage, allow_overwrite: bool = False):
        """
        Synchronously executes the underlying query and persists the result in the same offline store
        at the specified destination.

        Args:
            storage: The saved dataset storage object specifying where the result should be persisted.
            allow_overwrite: If True, a pre-existing location (e.g. table or file) can be overwritten.
                Currently not all individual offline store implementations make use of this parameter.
        """
        pass

    @property
    @abstractmethod
    def metadata(self) -> Optional[RetrievalMetadata]:
        """Returns metadata about the retrieval job."""
        pass

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
        raise NotImplementedError()


class OfflineStore(ABC):
    """
    An offline store defines the interface that Feast uses to interact with the storage and compute system that
    handles offline features.

    Each offline store implementation is designed to work only with the corresponding data source. For example,
    the SnowflakeOfflineStore can handle SnowflakeSources but not FileSources.
    """

    @staticmethod
    @abstractmethod
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
        pass

    @staticmethod
    @abstractmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
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
                provided or a SQL query.
            registry: The registry for the current feature store.
            project: Feast project to which the feature views belong.
            full_feature_names: If True, feature names will be prefixed with the corresponding feature view name,
                changing them from the format "feature" to "feature_view__feature" (e.g. "daily_transactions"
                changes to "customer_fv__daily_transactions").

        Returns:
            A RetrievalJob that can be executed to get the features.
        """
        pass

    @staticmethod
    @abstractmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
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
            timestamp_field: The timestamp column.
            start_date: The start of the time range.
            end_date: The end of the time range.

        Returns:
            A RetrievalJob that can be executed to get the entity rows.
        """
        pass

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
        raise NotImplementedError()

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
        raise NotImplementedError()
