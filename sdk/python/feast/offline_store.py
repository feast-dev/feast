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
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import pyarrow

from feast.big_query_source import BigQuerySource
from feast.feature_view import FeatureView
from feast.parquet_source import ParquetSource


class RetrievalJob(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def to_df(self):
        pass


class FileRetrievalJob(RetrievalJob):
    def __init__(self, evaluation_function):
        self.evaluation_function = evaluation_function

    def to_df(self):
        df = self.evaluation_function()
        return df


class OfflineStore(ABC):
    """
    OfflineStore is an object used for all interaction between Feast and the service used for offline storage of features. Currently BigQuery is supported.
    """

    @staticmethod
    @abstractmethod
    def pull_latest_from_table(
            feature_view: FeatureView, start_date: datetime, end_date: datetime,
    ) -> Optional[pyarrow.Table]:
        pass


class BigQueryOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table(
            feature_view: FeatureView, start_date: datetime, end_date: datetime,
    ) -> pyarrow.Table:
        if feature_view.input.table_ref is None:
            raise ValueError(
                "This function can only be called on a FeatureView with a table_ref"
            )

        (
            entity_names,
            feature_names,
            event_timestamp_column,
            created_timestamp_column,
        ) = run_reverse_field_mapping(feature_view)

        partition_by_entity_string = ", ".join(entity_names)
        if partition_by_entity_string != "":
            partition_by_entity_string = "PARTITION BY " + partition_by_entity_string
        timestamps = [event_timestamp_column]
        if created_timestamp_column is not None:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = ", ".join(entity_names + feature_names + timestamps)

        query = f"""
        SELECT {field_string}
        FROM (
            SELECT {field_string},
            ROW_NUMBER() OVER({partition_by_entity_string} ORDER BY {timestamp_desc_string}) AS _feast_row
            FROM `{feature_view.input.table_ref}`
            WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
        )
        WHERE _feast_row = 1
        """

        table = BigQueryOfflineStore._pull_query(query)
        table = run_forward_field_mapping(table, feature_view)
        return table

    @staticmethod
    def _pull_query(query: str) -> pyarrow.Table:
        from google.cloud import bigquery

        client = bigquery.Client()
        query_job = client.query(query)
        return query_job.to_arrow()

    @staticmethod
    def get_historical_features(
            feature_refs: List[str], entity_df: Union[pd.DataFrame], metadata,
    ) -> RetrievalJob:
        raise NotImplementedError()


class ParquetOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table(
            feature_view: FeatureView, start_date: datetime, end_date: datetime,
    ) -> pyarrow.Table:
        pass

    @staticmethod
    def get_historical_features(
            feature_refs: List[str], entity_df: Union[pd.DataFrame], metadata,
    ) -> RetrievalJob:
        # Get latest metadata (entities, feature views) from store
        # entities, feature_views = _get_latest_metadata()

        # Ensure that feature views have the correct reference structure (a.b)

        # Get list of feature views to query
        feature_views_to_query = {}
        for ref in feature_refs:
            ref_parts = ref.split(":")
            if ref_parts[0] not in metadata["feature_views"]:
                raise ValueError(f"Could not find feature view from reference {ref}")

            feature_view = metadata["feature_views"][ref_parts[0]]
            feature_views_to_query[feature_view.name] = feature_view

        # Ensure that we only read from the same source type
        if all(
                [
                    type(feature_view.inputs) == ParquetSource
                    for feature_view in feature_views_to_query.values()
                ]
        ) and isinstance(entity_df, pd.DataFrame):
            job = ParquetOfflineStore._get_historical_features_from_parquet(
                feature_refs, entity_df, metadata, feature_views_to_query
            )
        elif all(
                [
                    type(feature_view.inputs) == BigQuerySource
                    for feature_view in feature_views_to_query.values()
                ]
        ) and isinstance(entity_df, str):
            job = BigQueryOfflineStore._get_historical_features_from_parquet(
                feature_refs, entity_df, metadata, feature_views_to_query
            )
        else:
            raise NotImplementedError(
                f"Unsupported combination of feature view input source types and entity_df type. "
                f"Please ensure that all source types are consistent and are collocated in the same storage layer."
            )
        return job

    @staticmethod
    def _get_historical_features_from_parquet(
            feature_refs: List[str],
            entity_df: Union[pd.DataFrame],
            metadata,
            feature_views_to_query: Dict[str, FeatureView],
    ) -> FileRetrievalJob:
        def evaluate_historical_retrieval():

            # Sort entity dataframe prior to join, and create a copy to prevent modifying the original
            entity_df_with_features = entity_df.sort_values("datetime").copy()

            # Load feature view data from sources and join them incrementally
            for feature_view in feature_views_to_query.values():
                # Read dataframe to join to entity dataframe
                df_to_join = pd.read_parquet(feature_view.inputs.path).sort_values(
                    "datetime"
                )

                # Build a list of all the features we should select from this source
                feature_names = []
                for feature_ref in feature_refs:
                    feature_ref_feature_view_name = feature_ref.split(":")[0]
                    # Is the current feature ref within the current feature view?
                    if feature_ref_feature_view_name == feature_view.name:
                        feature_ref_feature_name = feature_ref.split(":")[1]

                        # Modify the separator for feature refs in column names to double underscore.
                        # We are using double underscore as separator for consistency with other databases like BigQuery,
                        # where there are very few characters available for use as separators
                        prefixed_feature_name = feature_ref.replace(":", "__")

                        # Add the feature name to the list of columns
                        feature_names.append(prefixed_feature_name)

                        # Ensure that the source dataframe feature column includes the feature view name as a prefix
                        df_to_join.rename(
                            columns={feature_ref_feature_name: prefixed_feature_name},
                            inplace=True,
                        )

                # Build a list of entity columns to join on (from the right table)
                right_entity_columns = [entity.name for entity in feature_view.entities]
                right_entity_key_columns = ["datetime"] + right_entity_columns

                # Remove all duplicate entity keys (using created timestamp)
                df_to_join.sort_values(
                    by=(right_entity_key_columns + ["created"]), inplace=True
                )
                df_to_join = df_to_join.groupby(by=(right_entity_key_columns)).last()
                df_to_join.reset_index(inplace=True)

                # Select only the columns we need to join from the feature dataframe
                df_to_join = df_to_join[right_entity_key_columns + feature_names]

                # Do point in-time-join between entity_df and feature dataframe
                entity_df_with_features = pd.merge_asof(
                    entity_df_with_features,
                    df_to_join,
                    on="datetime",
                    by=right_entity_columns,
                    tolerance=feature_view.get_ttl_as_timedelta(),
                )

                # Ensure that we delete dataframes to free up memory
                del df_to_join

            # Move "datetime" column to front
            current_cols = entity_df_with_features.columns.tolist()
            current_cols.remove("datetime")
            entity_df_with_features = entity_df_with_features[
                ["datetime"] + current_cols
                ]

            return entity_df_with_features

        job = FileRetrievalJob(evaluation_function=evaluate_historical_retrieval)
        return job


def run_reverse_field_mapping(
        feature_view: FeatureView,
) -> Tuple[List[str], List[str], str, Optional[str]]:
    """
    If a field mapping exists, run it in reverse on the entity names, feature names, event timestamp column, and created timestamp column to get the names of the relevant columns in the BigQuery table.

    Args:
        feature_view: FeatureView object containing the field mapping as well as the names to reverse-map.
    Returns:
        Tuple containing the list of reverse-mapped entity names, reverse-mapped feature names, reverse-mapped event timestamp column, and reverse-mapped created timestamp column that will be passed into the query to the offline store.
    """
    # if we have mapped fields, use the original field names in the call to the offline store
    event_timestamp_column = feature_view.input.event_timestamp_column
    entity_names = [entity for entity in feature_view.entities]
    feature_names = [feature.name for feature in feature_view.features]
    created_timestamp_column = feature_view.input.created_timestamp_column
    if feature_view.input.field_mapping is not None:
        reverse_field_mapping = {
            v: k for k, v in feature_view.input.field_mapping.items()
        }
        event_timestamp_column = (
            reverse_field_mapping[event_timestamp_column]
            if event_timestamp_column in reverse_field_mapping.keys()
            else event_timestamp_column
        )
        created_timestamp_column = (
            reverse_field_mapping[created_timestamp_column]
            if created_timestamp_column is not None
               and created_timestamp_column in reverse_field_mapping.keys()
            else created_timestamp_column
        )
        entity_names = [
            reverse_field_mapping[col] if col in reverse_field_mapping.keys() else col
            for col in entity_names
        ]
        feature_names = [
            reverse_field_mapping[col] if col in reverse_field_mapping.keys() else col
            for col in feature_names
        ]
    return (
        entity_names,
        feature_names,
        event_timestamp_column,
        created_timestamp_column,
    )


def run_forward_field_mapping(
        table: pyarrow.Table, feature_view: FeatureView
) -> pyarrow.Table:
    # run field mapping in the forward direction
    if table is not None and feature_view.input.field_mapping is not None:
        cols = table.column_names
        mapped_cols = [
            feature_view.input.field_mapping[col]
            if col in feature_view.input.field_mapping.keys()
            else col
            for col in cols
        ]
        table = table.rename_columns(mapped_cols)
    return table
