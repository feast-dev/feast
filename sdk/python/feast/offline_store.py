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
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Union

from dataclasses import asdict
from jinja2 import Environment, BaseLoader
import pandas as pd
import pyarrow
from dataclasses import dataclass
from google.cloud import bigquery

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


class BigQueryRetrievalJob(RetrievalJob):
    def __init__(self, query):
        self.query = query

    def to_df(self):
        # TODO: We should ideally be starting this job when the user runs "get_historical_features", not when
        #   they run to_df()
        client = bigquery.Client()
        df = client.query(self.query).to_dataframe(create_bqstorage_client=True)
        return df


@dataclass
class FeatureViewRequest:
    featureview: FeatureView
    features: List[str]


@dataclass
class FeatureViewQueryContext:
    project: str
    name: str
    ttl: int
    entities: List[str]
    features: List[str]  # feature reference
    table_ref: str
    event_timestamp_column: str
    created_timestamp_column: str
    field_mapping: Dict[str, str]
    query: str
    table_subquery: str


@dataclass
class FeatureViewJoinContext:
    entities: List[str]
    featureviews: List[FeatureViewQueryContext]
    leftTableName: str


def get_historical_features(
        metadata, feature_refs: List[str], entity_df: Union[pd.DataFrame, str],
) -> RetrievalJob:
    # TODO: Validate inputs

    feature_views_to_query = get_feature_views_from_feature_refs(feature_refs, metadata)

    # Retrieve features from ParquetOfflineStore
    if all(
            [
                type(feature_view.inputs) == ParquetSource
                for feature_view in feature_views_to_query.values()
            ]
    ) and isinstance(entity_df, pd.DataFrame):
        return ParquetOfflineStore.get_historical_features(
            metadata, feature_refs, entity_df,
        )

    # Retrieve features from BigQueryOfflineStore
    if all(
            [
                type(feature_view.inputs) == BigQuerySource
                for feature_view in feature_views_to_query.values()
            ]
    ) and isinstance(entity_df, str):
        return get_historical_features(
            metadata, feature_refs, entity_df,
        )

    # Could not map inputs to an OfflineStore implementation
    raise NotImplementedError(
        f"Unsupported combination of feature view input source types and entity_df type. "
        f"Please ensure that all source types are consistent and available in the same offline store."
    )


def get_feature_views_from_feature_refs(feature_refs, metadata):
    # Get map of feature views based on feature references
    feature_views_to_query = {}
    for ref in feature_refs:
        ref_parts = ref.split(":")
        if ref_parts[0] not in metadata["feature_views"]:
            raise ValueError(f"Could not find feature view from reference {ref}")
        feature_view = metadata["feature_views"][ref_parts[0]]
        feature_views_to_query[feature_view.name] = feature_view
    return feature_views_to_query


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

    @staticmethod
    @abstractmethod
    def get_historical_features(
            metadata, feature_refs: List[str], entity_df: Union[pd.DataFrame, str],
    ) -> RetrievalJob:
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


def build_point_in_time_query(
        feature_view_query_contexts: FeatureViewQueryContext,
        gcp_project_id: str,
        dataset_id: str,
        min_timestamp: datetime,
        max_timestamp: datetime,
        left_table_query_string: str):
    """Build point-in-time query between each feature view table and the entity dataframe"""
    template = Environment(loader=BaseLoader).from_string(source=SINGLE_FEATURE_VIEW_POINT_IN_TIME_JOIN)

    # Add additional fields to dict
    template_context = {
        "gcp_project_id": gcp_project_id,
        "dataset_id": dataset_id,
        "min_timestamp": min_timestamp,
        "max_timestamp": max_timestamp,
        "left_table_query_string": left_table_query_string,
        "featureviews": []
    }

    for feature_view_query_context in feature_view_query_contexts:
        template_context["featureviews"].append(asdict(feature_view_query_context))

    query = template.render(template_context)
    return query


SINGLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
WITH entity_dataframe AS (
    SELECT ROW_NUMBER() OVER() AS uuid, edf.* FROM kf-feast.test_hist_retrieval_static.orders as edf
),
{% for featureview in featureviews %}
/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.
 1. Concatenate the timestamp and entities from the feature set table with the entity dataset.
 Feature values are joined to this table later for improved efficiency.
 featureview_timestamp is equal to null in rows from the entity dataset.
 */
{{ featureview.name }}__union_features AS (
SELECT
  -- uuid is a unique identifier for each row in the entity dataset. Generated by `QueryTemplater.createEntityTableUUIDQuery`
  uuid,
  -- event_timestamp contains the timestamps to join onto
  event_timestamp,
  -- the feature_timestamp, i.e. the latest occurrence of the requested feature relative to the entity_dataset timestamp
  NULL as {{ featureview.name }}_feature_timestamp,
  -- created timestamp of the feature at the corresponding feature_timestamp
  NULL as created_timestamp,
  -- select only entities belonging to this feature set
  {{ featureview.entities | join(', ')}},
  -- boolean for filtering the dataset later
  true AS is_entity_table
FROM entity_dataframe
UNION ALL
SELECT
  NULL as uuid,
  {{ featureview.event_timestamp_column }} as event_timestamp,
  {{ featureview.event_timestamp_column }} as {{ featureview.name }}_feature_timestamp,
  {{ featureview.created_timestamp_column }} as created_timestamp,
  {{ featureview.entities | join(', ')}},
  false AS is_entity_table
FROM {{ featureview.table_subquery }} WHERE {{ featureview.event_timestamp_column }} <= '{{ max_timestamp }}'
{% if featureview.ttl == 0 %}{% else %}AND {{ featureview.event_timestamp_column }} >= Timestamp_sub(TIMESTAMP '{{ min_timestamp }}', interval {{ featureview.ttl }} second){% endif %}
),
/*
 2. Window the data in the unioned dataset, partitioning by entity and ordering by event_timestamp, as
 well as is_entity_table.
 Within each window, back-fill the feature_timestamp - as a result of this, the null feature_timestamps
 in the rows from the entity table should now contain the latest timestamps relative to the row's
 event_timestamp.
 For rows where event_timestamp(provided datetime) - feature_timestamp > max age, set the
 feature_timestamp to null.
 */
{{ featureview.name }}__joined AS (
SELECT
  uuid,
  event_timestamp,
  {{ featureview.entities | join(', ')}},
  {% for feature in featureview.features %}
  IF(event_timestamp >= {{ featureview.name }}_feature_timestamp {% if featureview.ttl == 0 %}{% else %}AND Timestamp_sub(event_timestamp, interval {{ featureview.ttl }} second) < {{ featureview.name }}_feature_timestamp{% endif %}, {{ featureview.name }}__{{ feature }}, NULL) as {{ featureview.name }}__{{ feature }}{% if loop.last %}{% else %}, {% endif %}
  {% endfor %}
FROM (
SELECT
  uuid,
  event_timestamp,
  {{ featureview.entities | join(', ')}},
  FIRST_VALUE(created_timestamp IGNORE NULLS) over w AS created_timestamp,
  FIRST_VALUE({{ featureview.name }}_feature_timestamp IGNORE NULLS) over w AS {{ featureview.name }}_feature_timestamp,
  is_entity_table
FROM {{ featureview.name }}__union_features
WINDOW w AS (PARTITION BY {{ featureview.entities | join(', ') }} ORDER BY event_timestamp DESC, is_entity_table DESC, created_timestamp DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
)
/*
 3. Select only the rows from the entity table, and join the features from the original feature set table
 to the dataset using the entity values, feature_timestamp, and created_timestamps.
 */
LEFT JOIN (
SELECT
  {{ featureview.event_timestamp_column }} as {{ featureview.name }}_feature_timestamp,
  {{ featureview.created_timestamp_column }} as created_timestamp,
  {{ featureview.entities | join(', ')}},
  {% for feature in featureview.features %}
  {{ feature }} as {{ featureview.name }}__{{ feature }}{% if loop.last %}{% else %}, {% endif %}
  {% endfor %}
FROM {{ featureview.table_subquery }} WHERE {{ featureview.event_timestamp_column }} <= '{{ max_timestamp }}'
{% if featureview.ttl == 0 %}{% else %}AND {{ featureview.event_timestamp_column }} >= Timestamp_sub(TIMESTAMP '{{ min_timestamp }}', interval {{ featureview.ttl }} second){% endif %}
) USING ({{ featureview.name }}_feature_timestamp, created_timestamp, {{ featureview.entities | join(', ')}})
WHERE is_entity_table
),
/*
 4. Finally, deduplicate the rows by selecting the first occurrence of each entity table row UUID.
 */
{{ featureview.name }}__deduped AS (SELECT
  k.*
FROM (
  SELECT ARRAY_AGG(row LIMIT 1)[OFFSET(0)] k
  FROM {{ featureview.name }}__joined row
  GROUP BY uuid
)){% if loop.last %}{% else %}, {% endif %}

{% endfor %}
/*
 Joins the outputs of multiple point-in-time-correctness joins to a single table.
 */
SELECT * FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
    uuid,
    {% for feature in featureview.features %}
    {{ featureview.name }}__{{ feature }}{% if loop.last %}{% else %}, {% endif %}
    {% endfor %}
    FROM {{ featureview.name }}__deduped
) USING (uuid)
{% endfor %}
"""


def get_historical_features(
        metadata, feature_refs: List[str], entity_df: Union[pd.DataFrame, str],
) -> RetrievalJob:
    if type(entity_df) is not str:
        raise NotImplementedError("Only string queries are allowed for providing an entity dataframe.")
    left_table_query_string = f"({entity_df})"

    feature_views_to_query = get_feature_view_requests(feature_refs, metadata)
    feature_view_query_contexts = get_feature_view_query_contexts(feature_views_to_query)
    # TODO: 1. Add support for loading a Pandas entity_df into BigQuery

    # 2. Retrieve the temporal bounds of the entity dataset provided

    query = build_point_in_time_query(
        feature_view_query_contexts,
        gcp_project_id="default",
        # TODO: This project should come from main config or env, and is differnt from sources
        dataset_id="test_hist_retrieval_static",  # TODO: This dataset_id shouldn't be hardcoded
        min_timestamp=datetime.now() - timedelta(days=365),  # TODO: Get timestamps from entity_df
        max_timestamp=datetime.now() + timedelta(days=1),
        left_table_query_string=left_table_query_string
    )
    job = BigQueryRetrievalJob(query=query)
    return job


def get_feature_view_requests(feature_refs, metadata):
    # Get map of feature views based on feature references
    feature_views_to_feature_map = {}
    for ref in feature_refs:
        ref_parts = ref.split(":")
        if ref_parts[0] not in metadata["feature_views"]:
            raise ValueError(f"Could not find feature view from reference {ref}")
        feature_view = metadata["feature_views"][ref_parts[0]]

        feature = ref_parts[1]
        if feature_view in feature_views_to_feature_map:
            feature_views_to_feature_map[feature_view].append(feature)
        else:
            feature_views_to_feature_map[feature_view] = [feature]

    feature_view_requests = []
    for view, features in feature_views_to_feature_map.items():
        feature_view_requests.append(FeatureViewRequest(view, features))
    return feature_view_requests


def get_feature_view_query_contexts(feature_view_requests: List[FeatureViewRequest]):
    contexts = []
    for request in feature_view_requests:
        entity_names = [entity.name for entity in request.featureview.entities]
        ttl_seconds = int(request.featureview.get_ttl_as_timedelta().total_seconds())

        if request.featureview.inputs.table_ref is not None:
            table_subquery = f"`{request.featureview.inputs.table_ref}`"
        else:
            table_subquery = f"({request.featureview.inputs.query})"

        # TODO: Project has been hardcoded here. Needs to come from metadata store
        context = FeatureViewQueryContext(
            project="default",
            name=request.featureview.name,
            ttl=ttl_seconds,
            entities=entity_names,
            features=request.features,
            table_ref=request.featureview.inputs.table_ref,
            event_timestamp_column=request.featureview.inputs.event_timestamp_column,
            created_timestamp_column="created",  # TODO: Make created column optional and not hardcoded
            field_mapping=request.featureview.inputs.field_mapping,
            query=request.featureview.inputs.query,
            table_subquery=table_subquery
        )
        contexts.append(context)
    return contexts


class ParquetOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table(
            feature_view: FeatureView, start_date: datetime, end_date: datetime,
    ) -> pyarrow.Table:
        pass

    @staticmethod
    def get_historical_features(
            metadata,
            feature_refs: List[str],
            entity_df: Union[pd.DataFrame, str]
    ) -> FileRetrievalJob:
        feature_views_to_query = get_feature_views_from_feature_refs(feature_refs, metadata)

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


JOIN_FEATURE_VIEWS_SQL_TEMPLATE = """
/*
 Joins the outputs of multiple point-in-time-correctness joins to a single table.
 */
WITH joined as (
SELECT * FROM `{{ left_table_name }}`
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
    uuid,
    {% for feature in featureview.features %}
    {{ featureview.name }}__{{ feature }}{% if loop.last %}{% else %}, {% endif %}
    {% endfor %}
    FROM `{{ featureview.table }}`
) USING (uuid)
{% endfor %}
) SELECT
    event_timestamp,
    {{ entities | join(', ') }}
    {% for featureview in featureviews %}
    {% for feature in featureview.features %}
	,{{ featureview.name }}__{{ feature }} as {% if feature.featureview != "" %}{{ featureview.name }}__{% endif %}{{ feature }}
    {% endfor %}
    {% endfor %}
FROM joined

"""
