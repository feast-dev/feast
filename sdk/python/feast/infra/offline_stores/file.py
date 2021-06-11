from datetime import datetime
from typing import Callable, List, Optional, Union

import pandas as pd
import pyarrow
import pytz

from feast.data_source import DataSource, FileSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.provider import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
    _get_requested_feature_views_to_features_dict,
    _run_field_mapping,
)
from feast.registry import Registry
from feast.repo_config import RepoConfig


class FileRetrievalJob(RetrievalJob):
    def __init__(self, evaluation_function: Callable):
        """Initialize a lazy historical retrieval job"""

        # The evaluation function executes a stored procedure to compute a historical retrieval.
        self.evaluation_function = evaluation_function

    def to_df(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function()
        return df


class FileOfflineStore(OfflineStore):
    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
    ) -> FileRetrievalJob:
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

        feature_views_to_features = _get_requested_feature_views_to_features_dict(
            feature_refs, feature_views
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
                event_timestamp_column = feature_view.input.event_timestamp_column
                created_timestamp_column = feature_view.input.created_timestamp_column

                # Read offline parquet data in pyarrow format
                table = pyarrow.parquet.read_table(feature_view.input.path)

                # Rename columns by the field mapping dictionary if it exists
                if feature_view.input.field_mapping is not None:
                    table = _run_field_mapping(table, feature_view.input.field_mapping)

                # Convert pyarrow table to pandas dataframe
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
                    prefixed_feature_name = f"{feature_view.name}__{feature}"

                    # Add the feature name to the list of columns
                    feature_names.append(prefixed_feature_name)

                    # Ensure that the source dataframe feature column includes the feature view name as a prefix
                    df_to_join.rename(
                        columns={feature: prefixed_feature_name}, inplace=True,
                    )

                # Build a list of entity columns to join on (from the right table)
                join_keys = []
                for entity_name in feature_view.entities:
                    entity = registry.get_entity(entity_name, project)
                    join_keys.append(entity.join_key)
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
                    by=right_entity_columns,
                    tolerance=feature_view.ttl,
                )

                # Remove right (feature table/view) event_timestamp column.
                if event_timestamp_column != entity_df_event_timestamp_col:
                    entity_df_with_features.drop(
                        columns=[event_timestamp_column], inplace=True
                    )

                # Ensure that we delete dataframes to free up memory
                del df_to_join

            # Move "datetime" column to front
            current_cols = entity_df_with_features.columns.tolist()
            current_cols.remove(entity_df_event_timestamp_col)
            entity_df_with_features = entity_df_with_features[
                [entity_df_event_timestamp_col] + current_cols
            ]

            return entity_df_with_features

        job = FileRetrievalJob(evaluation_function=evaluate_historical_retrieval)
        return job

    @staticmethod
    def pull_latest_from_table_or_query(
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> pyarrow.Table:
        assert isinstance(data_source, FileSource)

        source_df = pd.read_parquet(data_source.path)
        # Make sure all timestamp fields are tz-aware. We default tz-naive fields to UTC
        source_df[event_timestamp_column] = source_df[event_timestamp_column].apply(
            lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc)
        )
        if created_timestamp_column:
            source_df[created_timestamp_column] = source_df[
                created_timestamp_column
            ].apply(lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc))

        source_columns = set(source_df.columns)
        if not set(join_key_columns).issubset(source_columns):
            raise ValueError(
                f"The DataFrame must have at least {set(join_key_columns)} columns present, "
                f"but these were missing: {set(join_key_columns)- source_columns} "
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
        last_values_df = filtered_df.drop_duplicates(
            join_key_columns, keep="last", ignore_index=True
        )

        columns_to_extract = set(join_key_columns + feature_name_columns + ts_columns)
        table = pyarrow.Table.from_pandas(last_values_df[columns_to_extract])

        return table
