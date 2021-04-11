from datetime import datetime
from typing import Callable, List, Optional, Union

import pandas as pd
import pyarrow
import pytz

from feast.data_source import DataSource, FileSource
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.provider import (
    ENTITY_DF_EVENT_TIMESTAMP_COL,
    _get_requested_feature_views_to_features_dict,
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

        feature_views_to_features = _get_requested_feature_views_to_features_dict(
            feature_refs, feature_views
        )

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_historical_retrieval():

            # Sort entity dataframe prior to join, and create a copy to prevent modifying the original
            entity_df_with_features = entity_df.sort_values(
                ENTITY_DF_EVENT_TIMESTAMP_COL
            ).copy()

            # Load feature view data from sources and join them incrementally
            for feature_view, features in feature_views_to_features.items():
                event_timestamp_column = feature_view.input.event_timestamp_column
                created_timestamp_column = feature_view.input.created_timestamp_column

                # Read dataframe to join to entity dataframe
                df_to_join = pd.read_parquet(feature_view.input.path).sort_values(
                    event_timestamp_column
                )

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
                df_to_join = df_to_join.groupby(by=right_entity_key_columns).last()
                df_to_join.reset_index(inplace=True)

                # Select only the columns we need to join from the feature dataframe
                df_to_join = df_to_join[right_entity_key_columns + feature_names]

                # Do point in-time-join between entity_df and feature dataframe
                entity_df_with_features = pd.merge_asof(
                    entity_df_with_features,
                    df_to_join,
                    left_on=ENTITY_DF_EVENT_TIMESTAMP_COL,
                    right_on=event_timestamp_column,
                    by=right_entity_columns,
                    tolerance=feature_view.ttl,
                )

                # Remove right (feature table/view) event_timestamp column.
                if event_timestamp_column != ENTITY_DF_EVENT_TIMESTAMP_COL:
                    entity_df_with_features.drop(
                        columns=[event_timestamp_column], inplace=True
                    )

                # Ensure that we delete dataframes to free up memory
                del df_to_join

            # Move "datetime" column to front
            current_cols = entity_df_with_features.columns.tolist()
            current_cols.remove(ENTITY_DF_EVENT_TIMESTAMP_COL)
            entity_df_with_features = entity_df_with_features[
                [ENTITY_DF_EVENT_TIMESTAMP_COL] + current_cols
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
            lambda x: x if x.tz is not None else x.replace(tzinfo=pytz.utc)
        )
        source_df[created_timestamp_column] = source_df[created_timestamp_column].apply(
            lambda x: x if x.tz is not None else x.replace(tzinfo=pytz.utc)
        )

        ts_columns = (
            [event_timestamp_column, created_timestamp_column]
            if created_timestamp_column is not None
            else [event_timestamp_column]
        )

        source_df.sort_values(by=ts_columns, inplace=True)

        filtered_df = source_df[
            (source_df[event_timestamp_column] >= start_date)
            & (source_df[event_timestamp_column] < end_date)
        ]
        last_values_df = filtered_df.groupby(by=join_key_columns).last()

        # make driver_id a normal column again
        last_values_df.reset_index(inplace=True)

        table = pyarrow.Table.from_pandas(
            last_values_df[join_key_columns + feature_name_columns + ts_columns]
        )

        return table
