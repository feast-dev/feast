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
from typing import List, Optional, Tuple

import pyarrow

from feast.feature_view import FeatureView


class OfflineStore(ABC):
    """
    OfflineStore is a non-user-facing object used for all interaction between Feast and the service used for offline storage of features. Currently BigQuery is supported.
    """

    @staticmethod
    @abstractmethod
    def pull_latest_from_table(
        table_ref: str,
        entity_names: List[str],
        feature_names: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> Optional[pyarrow.Table]:
        pass


class BigQueryOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table(
        table_ref: str,
        entity_names: List[str],
        feature_names: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> pyarrow.Table:

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
            FROM `{table_ref}`
            WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
        )
        WHERE _feast_row = 1
        """
        return BigQueryOfflineStore._pull_query(query)

    @staticmethod
    def _pull_query(query: str) -> pyarrow.Table:
        from google.cloud import bigquery

        client = bigquery.Client()
        query_job = client.query(query)
        return query_job.to_arrow()


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
    event_timestamp_column = feature_view.inputs.event_timestamp_column
    entity_names = [entity.name for entity in feature_view.entities]
    feature_names = [feature.name for feature in feature_view.features]
    created_timestamp_column = feature_view.inputs.created_timestamp_column
    if feature_view.inputs.field_mapping is not None:
        reverse_field_mapping = {
            v: k for k, v in feature_view.inputs.field_mapping.items()
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
    if table is not None and feature_view.inputs.field_mapping is not None:
        cols = table.column_names
        mapped_cols = [
            feature_view.inputs.field_mapping[col]
            if col in feature_view.inputs.field_mapping.keys()
            else col
            for col in cols
        ]
        table = table.rename_columns(mapped_cols)
    return table
