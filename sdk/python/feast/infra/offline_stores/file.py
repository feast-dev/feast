from datetime import datetime
from typing import Callable, List, Optional, Tuple, Union

import pandas as pd
import pyarrow
import pytz
from pydantic.typing import Literal

from feast import FileSource, OnDemandFeatureView
from feast.data_source import DataSource
from feast.errors import FeastJoinKeysDuringMaterialization
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
)
from feast.infra.provider import (
    _get_requested_feature_views_to_features_dict,
    _run_field_mapping,
)
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.usage import log_exceptions_and_usage


class FileOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for local (file-based) store """

    type: Literal["file"] = "file"
    """ Offline store type selector"""


class FileRetrievalJob(RetrievalJob):
    def __init__(
        self,
        evaluation_function: Callable,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        """Initialize a lazy historical retrieval job"""

        # The evaluation function executes a stored procedure to compute a historical retrieval.
        self.evaluation_function = evaluation_function
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = (
            on_demand_feature_views if on_demand_feature_views else []
        )
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> Optional[List[OnDemandFeatureView]]:
        return self._on_demand_feature_views

    @log_exceptions_and_usage
    def _to_df_internal(self) -> pd.DataFrame:
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function()
        return df

    @log_exceptions_and_usage
    def _to_arrow_internal(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function()
        return pyarrow.Table.from_pandas(df)

    def persist(self, storage: SavedDatasetStorage):
        assert isinstance(storage, SavedDatasetFileStorage)

        filesystem, path = FileSource.create_filesystem_and_path(
            storage.file_options.file_url, storage.file_options.s3_endpoint_override,
        )

        if path.endswith(".parquet"):
            pyarrow.parquet.write_table(
                self.to_arrow(), where=path, filesystem=filesystem
            )
        else:
            # otherwise assume destination is directory
            pyarrow.parquet.write_to_dataset(
                self.to_arrow(), root_path=path, filesystem=filesystem
            )

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata


class FileOfflineStore(OfflineStore):
    @staticmethod
    @log_exceptions_and_usage(offline_store="file")
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        if not isinstance(entity_df, pd.DataFrame):
            raise ValueError(
                f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
            )
        entity_df_event_timestamp_col = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL  # local modifiable copy of global variable
        if entity_df_event_timestamp_col not in entity_df.columns:
            datetime_columns = entity_df.select_dtypes(
                include=["datetime", "datetimetz"]
            ).columns
            if len(datetime_columns) == 1:
                print(
                    f"Using {datetime_columns[0]} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
                )
                entity_df_event_timestamp_col = datetime_columns[0]
            else:
                raise ValueError(
                    f"Please provide an entity_df with a column named {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL} representing the time of events."
                )
        (
            feature_views_to_features,
            on_demand_feature_views_to_features,
        ) = _get_requested_feature_views_to_features_dict(
            feature_refs,
            feature_views,
            registry.list_on_demand_feature_views(config.project),
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df, entity_df_event_timestamp_col
        )

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_historical_retrieval():

            # Make sure all event timestamp fields are tz-aware. We default tz-naive fields to UTC
            entity_df[entity_df_event_timestamp_col] = entity_df[
                entity_df_event_timestamp_col
            ].apply(lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc))

            # Create a copy of entity_df to prevent modifying the original
            entity_df_with_features = entity_df.copy()

            # Convert event timestamp column to datetime and normalize time zone to UTC
            # This is necessary to avoid issues with pd.merge_asof
            entity_df_with_features[entity_df_event_timestamp_col] = pd.to_datetime(
                entity_df_with_features[entity_df_event_timestamp_col], utc=True
            )

            # Sort event timestamp values
            entity_df_with_features = entity_df_with_features.sort_values(
                entity_df_event_timestamp_col
            )

            # Load feature view data from sources and join them incrementally
            for feature_view, features in feature_views_to_features.items():
                event_timestamp_column = (
                    feature_view.batch_source.event_timestamp_column
                )
                created_timestamp_column = (
                    feature_view.batch_source.created_timestamp_column
                )

                # Read offline parquet data in pyarrow format.
                filesystem, path = FileSource.create_filesystem_and_path(
                    feature_view.batch_source.path,
                    feature_view.batch_source.file_options.s3_endpoint_override,
                )
                table = pyarrow.parquet.read_table(path, filesystem=filesystem)

                # Rename columns by the field mapping dictionary if it exists
                if feature_view.batch_source.field_mapping is not None:
                    table = _run_field_mapping(
                        table, feature_view.batch_source.field_mapping
                    )
                # Rename entity columns by the join_key_map dictionary if it exists
                if feature_view.projection.join_key_map:
                    table = _run_field_mapping(
                        table, feature_view.projection.join_key_map
                    )

                # Convert pyarrow table to pandas dataframe. Note, if the underlying data has missing values,
                # pandas will convert those values to np.nan if the dtypes are numerical (floats, ints, etc.) or boolean
                # If the dtype is 'object', then missing values are inferred as python `None`s.
                # More details at:
                # https://pandas.pydata.org/pandas-docs/stable/user_guide/missing_data.html#values-considered-missing
                df_to_join = table.to_pandas()

                # Make sure all timestamp fields are tz-aware. We default tz-naive fields to UTC
                df_to_join[event_timestamp_column] = df_to_join[
                    event_timestamp_column
                ].apply(
                    lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc)
                )
                if created_timestamp_column:
                    df_to_join[created_timestamp_column] = df_to_join[
                        created_timestamp_column
                    ].apply(
                        lambda x: x
                        if x.tzinfo is not None
                        else x.replace(tzinfo=pytz.utc)
                    )

                # Sort dataframe by the event timestamp column
                df_to_join = df_to_join.sort_values(event_timestamp_column)

                # Build a list of all the features we should select from this source
                feature_names = []
                for feature in features:
                    # Modify the separator for feature refs in column names to double underscore. We are using
                    # double underscore as separator for consistency with other databases like BigQuery,
                    # where there are very few characters available for use as separators
                    if full_feature_names:
                        formatted_feature_name = (
                            f"{feature_view.projection.name_to_use()}__{feature}"
                        )
                    else:
                        formatted_feature_name = feature
                    # Add the feature name to the list of columns
                    feature_names.append(formatted_feature_name)

                    # Ensure that the source dataframe feature column includes the feature view name as a prefix
                    df_to_join.rename(
                        columns={feature: formatted_feature_name}, inplace=True,
                    )

                # Build a list of entity columns to join on (from the right table)
                join_keys = []
                for entity_name in feature_view.entities:
                    entity = registry.get_entity(entity_name, project)
                    join_key = feature_view.projection.join_key_map.get(
                        entity.join_key, entity.join_key
                    )
                    join_keys.append(join_key)
                right_entity_columns = join_keys
                right_entity_key_columns = [
                    event_timestamp_column
                ] + right_entity_columns

                # Remove all duplicate entity keys (using created timestamp)
                right_entity_key_sort_columns = right_entity_key_columns
                if created_timestamp_column:
                    # If created_timestamp is available, use it to dedupe deterministically
                    right_entity_key_sort_columns = right_entity_key_sort_columns + [
                        created_timestamp_column
                    ]

                df_to_join.sort_values(by=right_entity_key_sort_columns, inplace=True)
                df_to_join.drop_duplicates(
                    right_entity_key_sort_columns,
                    keep="last",
                    ignore_index=True,
                    inplace=True,
                )

                # Select only the columns we need to join from the feature dataframe
                df_to_join = df_to_join[right_entity_key_columns + feature_names]

                # Do point in-time-join between entity_df and feature dataframe
                entity_df_with_features = pd.merge_asof(
                    entity_df_with_features,
                    df_to_join,
                    left_on=entity_df_event_timestamp_col,
                    right_on=event_timestamp_column,
                    by=right_entity_columns or None,
                    tolerance=feature_view.ttl,
                )

                # Remove right (feature table/view) event_timestamp column.
                if event_timestamp_column != entity_df_event_timestamp_col:
                    entity_df_with_features.drop(
                        columns=[event_timestamp_column], inplace=True
                    )

                # Ensure that we delete dataframes to free up memory
                del df_to_join

            # Move "event_timestamp" column to front
            current_cols = entity_df_with_features.columns.tolist()
            current_cols.remove(entity_df_event_timestamp_col)
            entity_df_with_features = entity_df_with_features[
                [entity_df_event_timestamp_col] + current_cols
            ]

            return entity_df_with_features

        job = FileRetrievalJob(
            evaluation_function=evaluate_historical_retrieval,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(set(entity_df.columns) - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )
        return job

    @staticmethod
    @log_exceptions_and_usage(offline_store="file")
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(data_source, FileSource)

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_offline_job():
            filesystem, path = FileSource.create_filesystem_and_path(
                data_source.path, data_source.file_options.s3_endpoint_override
            )
            source_df = pd.read_parquet(path, filesystem=filesystem)
            # Make sure all timestamp fields are tz-aware. We default tz-naive fields to UTC
            source_df[event_timestamp_column] = source_df[event_timestamp_column].apply(
                lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc)
            )
            if created_timestamp_column:
                source_df[created_timestamp_column] = source_df[
                    created_timestamp_column
                ].apply(
                    lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc)
                )

            source_columns = set(source_df.columns)
            if not set(join_key_columns).issubset(source_columns):
                raise FeastJoinKeysDuringMaterialization(
                    data_source.path, set(join_key_columns), source_columns
                )

            ts_columns = (
                [event_timestamp_column, created_timestamp_column]
                if created_timestamp_column
                else [event_timestamp_column]
            )

            source_df.sort_values(by=ts_columns, inplace=True)

            filtered_df = source_df[
                (source_df[event_timestamp_column] >= start_date)
                & (source_df[event_timestamp_column] < end_date)
            ]

            columns_to_extract = set(
                join_key_columns + feature_name_columns + ts_columns
            )
            if join_key_columns:
                last_values_df = filtered_df.drop_duplicates(
                    join_key_columns, keep="last", ignore_index=True
                )
            else:
                last_values_df = filtered_df
                last_values_df[DUMMY_ENTITY_ID] = DUMMY_ENTITY_VAL
                columns_to_extract.add(DUMMY_ENTITY_ID)

            return last_values_df[columns_to_extract]

        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return FileRetrievalJob(
            evaluation_function=evaluate_offline_job, full_feature_names=False,
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="file")
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        return FileOfflineStore.pull_latest_from_table_or_query(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns
            + [event_timestamp_column],  # avoid deduplication
            feature_name_columns=feature_name_columns,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=None,
            start_date=start_date,
            end_date=end_date,
        )


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str], entity_df_event_timestamp_col: str,
) -> Tuple[datetime, datetime]:
    if not isinstance(entity_df, pd.DataFrame):
        raise ValueError(
            f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
        )

    entity_df_event_timestamp = entity_df.loc[
        :, entity_df_event_timestamp_col
    ].infer_objects()
    if pd.api.types.is_string_dtype(entity_df_event_timestamp):
        entity_df_event_timestamp = pd.to_datetime(entity_df_event_timestamp, utc=True)

    return (
        entity_df_event_timestamp.min().to_pydatetime(),
        entity_df_event_timestamp.max().to_pydatetime(),
    )
