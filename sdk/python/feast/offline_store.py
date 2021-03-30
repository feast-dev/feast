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
import time
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Type, Union

import pandas as pd
import pyarrow
from google.cloud import bigquery
from jinja2 import BaseLoader, Environment

from feast.data_source import BigQuerySource, DataSource, FileSource
from feast.feature_view import FeatureView
from feast.repo_config import RepoConfig

ENTITY_DF_EVENT_TIMESTAMP_COL = "event_timestamp"


class RetrievalJob(ABC):
    """RetrievalJob is used to manage the execution of a historical feature retrieval"""

    @abstractmethod
    def to_df(self):
        """Return dataset as Pandas DataFrame synchronously"""
        pass


class FileRetrievalJob(RetrievalJob):
    def __init__(self, evaluation_function: Callable):
        """Initialize a lazy historical retrieval job"""

        # The evaluation function executes a stored procedure to compute a historical retrieval.
        self.evaluation_function = evaluation_function

    def to_df(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function()
        return df


class BigQueryRetrievalJob(RetrievalJob):
    def __init__(self, query):
        self.query = query

    def to_df(self):
        # TODO: Ideally only start this job when the user runs "get_historical_features", not when they run to_df()
        client = bigquery.Client()
        df = client.query(self.query).to_dataframe(create_bqstorage_client=True)
        return df


@dataclass(frozen=True)
class FeatureViewQueryContext:
    """Context object used to template a BigQuery point-in-time SQL query"""

    name: str
    ttl: int
    entities: List[str]
    features: List[str]  # feature reference format
    table_ref: str
    event_timestamp_column: str
    created_timestamp_column: str
    field_mapping: Dict[str, str]
    query: str
    table_subquery: str


class OfflineStore(ABC):
    """
    OfflineStore is an object used for all interaction between Feast and the service used for offline storage of
    features. Currently BigQuery is supported.
    """

    @staticmethod
    @abstractmethod
    def pull_latest_from_table_or_query(
        data_source: DataSource,
        entity_names: List[str],
        feature_names: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> pyarrow.Table:
        """
        Note that entity_names, feature_names, event_timestamp_column, and created_timestamp_column
        have all already been mapped back to column names of the source table
        and those column names are the values passed into this function.
        """
        pass

    @staticmethod
    @abstractmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
    ) -> RetrievalJob:
        pass


class BigQueryOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table_or_query(
        data_source: DataSource,
        entity_names: List[str],
        feature_names: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> pyarrow.Table:
        assert isinstance(data_source, BigQuerySource)
        from_expression = data_source.get_table_query_string()

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
            FROM {from_expression}
            WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
        )
        WHERE _feast_row = 1
        """

        table = BigQueryOfflineStore._pull_query(query)
        return table

    @staticmethod
    def _pull_query(query: str) -> pyarrow.Table:
        from google.cloud import bigquery

        client = bigquery.Client()
        query_job = client.query(query)
        return query_job.to_arrow()

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
    ) -> RetrievalJob:
        # TODO: Add entity_df validation in order to fail before interacting with BigQuery

        if type(entity_df) is str:
            entity_df_sql_table = f"({entity_df})"
        elif isinstance(entity_df, pd.DataFrame):
            table_id = _upload_entity_df_into_bigquery(config.project, entity_df)
            entity_df_sql_table = f"`{table_id}`"
        else:
            raise ValueError(
                f"The entity dataframe you have provided must be a Pandas DataFrame or BigQuery SQL query, "
                f"but we found: {type(entity_df)} "
            )

        # Build a query context containing all information required to template the BigQuery SQL query
        query_context = get_feature_view_query_context(feature_refs, feature_views)

        # TODO: Infer min_timestamp and max_timestamp from entity_df
        # Generate the BigQuery SQL query from the query context
        query = build_point_in_time_query(
            query_context,
            min_timestamp=datetime.now() - timedelta(days=365),
            max_timestamp=datetime.now() + timedelta(days=1),
            left_table_query_string=entity_df_sql_table,
        )
        job = BigQueryRetrievalJob(query=query)
        return job


def _upload_entity_df_into_bigquery(project, entity_df) -> str:
    """Uploads a Pandas entity dataframe into a BigQuery table and returns a reference to the resulting table"""
    client = bigquery.Client()

    # First create the BigQuery dataset if it doesn't exist
    dataset = bigquery.Dataset(f"{client.project}.feast_{project}")
    dataset.location = "US"
    client.create_dataset(
        dataset, exists_ok=True
    )  # TODO: Consider moving this to apply or BigQueryOfflineStore

    # Drop the index so that we dont have unnecessary columns
    entity_df.reset_index(drop=True, inplace=True)

    # Upload the dataframe into BigQuery, creating a temporary table
    job_config = bigquery.LoadJobConfig()
    table_id = f"{client.project}.feast_{project}.entity_df_{int(time.time())}"
    job = client.load_table_from_dataframe(entity_df, table_id, job_config=job_config,)
    job.result()

    # Ensure that the table expires after some time
    table = client.get_table(table=table_id)
    table.expires = datetime.utcnow() + timedelta(minutes=30)
    client.update_table(table, ["expires"])

    return table_id


def get_feature_view_query_context(
    feature_refs: List[str], feature_views: List[FeatureView]
) -> List[FeatureViewQueryContext]:
    """Build a query context containing all information required to template a BigQuery point-in-time SQL query"""

    feature_views_to_feature_map = _get_requested_feature_views_to_features_dict(
        feature_refs, feature_views
    )

    query_context = []
    for feature_view, features in feature_views_to_feature_map.items():
        entity_names = [entity for entity in feature_view.entities]

        if isinstance(feature_view.ttl, timedelta):
            ttl_seconds = int(feature_view.ttl.total_seconds())
        else:
            ttl_seconds = 0

        assert isinstance(feature_view.input, BigQuerySource)

        context = FeatureViewQueryContext(
            name=feature_view.name,
            ttl=ttl_seconds,
            entities=entity_names,
            features=features,
            table_ref=feature_view.input.table_ref,
            event_timestamp_column=feature_view.input.event_timestamp_column,
            created_timestamp_column=feature_view.input.created_timestamp_column,
            # TODO: Make created column optional and not hardcoded
            field_mapping=feature_view.input.field_mapping,
            query=feature_view.input.query,
            table_subquery=feature_view.input.get_table_query_string(),
        )
        query_context.append(context)
    return query_context


def build_point_in_time_query(
    feature_view_query_contexts: List[FeatureViewQueryContext],
    min_timestamp: datetime,
    max_timestamp: datetime,
    left_table_query_string: str,
):
    """Build point-in-time query between each feature view table and the entity dataframe"""
    template = Environment(loader=BaseLoader()).from_string(
        source=SINGLE_FEATURE_VIEW_POINT_IN_TIME_JOIN
    )

    # Add additional fields to dict
    template_context = {
        "min_timestamp": min_timestamp,
        "max_timestamp": max_timestamp,
        "left_table_query_string": left_table_query_string,
        "featureviews": [asdict(context) for context in feature_view_query_contexts],
    }

    query = template.render(template_context)
    return query


class FileOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table_or_query(
        data_source: DataSource,
        entity_names: List[str],
        feature_names: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> pyarrow.Table:
        assert isinstance(data_source, FileSource)
        source_df = pd.read_parquet(data_source.path)

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
        last_values_df = filtered_df.groupby(by=entity_names).last()

        # make driver_id a normal column again
        last_values_df.reset_index(inplace=True)

        return pyarrow.Table.from_pandas(
            last_values_df[entity_names + feature_names + ts_columns]
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
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
                right_entity_columns = [entity for entity in feature_view.entities]
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


def get_offline_store_for_retrieval(feature_views: List[FeatureView],) -> OfflineStore:
    """Detect which offline store should be used for retrieving historical features"""

    source_types = [type(feature_view.input) for feature_view in feature_views]

    # Retrieve features from ParquetOfflineStore
    if all(source == FileSource for source in source_types):
        return FileOfflineStore()

    # Retrieve features from BigQueryOfflineStore
    if all(source == BigQuerySource for source in source_types):
        return BigQueryOfflineStore()

    # Could not map inputs to an OfflineStore implementation
    raise NotImplementedError(
        "Unsupported combination of feature view input source types. Please ensure that all source types are "
        "consistent and available in the same offline store."
    )


def _get_requested_feature_views_to_features_dict(
    feature_refs: List[str], feature_views: List[FeatureView]
) -> Dict[FeatureView, List[str]]:
    """Create a dict of FeatureView -> List[Feature] for all requested features"""

    feature_views_to_feature_map = {}  # type: Dict[FeatureView, List[str]]
    for ref in feature_refs:
        ref_parts = ref.split(":")
        feature_view_from_ref = ref_parts[0]
        feature_from_ref = ref_parts[1]
        found = False
        for feature_view_from_registry in feature_views:
            if feature_view_from_registry.name == feature_view_from_ref:
                found = True
                if feature_view_from_registry in feature_views_to_feature_map:
                    feature_views_to_feature_map[feature_view_from_registry].append(
                        feature_from_ref
                    )
                else:
                    feature_views_to_feature_map[feature_view_from_registry] = [
                        feature_from_ref
                    ]

        if not found:
            raise ValueError(f"Could not find feature view from reference {ref}")
    return feature_views_to_feature_map


# TODO: Optimizations
#   * Use GENERATE_UUID() instead of ROW_NUMBER(), or join on entity columns directly
#   * Precompute ROW_NUMBER() so that it doesn't have to be recomputed for every query on entity_dataframe
#   * Create temporary tables instead of keeping all tables in memory

SINGLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
WITH entity_dataframe AS (
    SELECT ROW_NUMBER() OVER() AS row_number, edf.* FROM {{ left_table_query_string }} as edf
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
  -- unique identifier for each row in the entity dataset.
  row_number,
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
  NULL as row_number,
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
  row_number,
  event_timestamp,
  {{ featureview.entities | join(', ')}},
  {% for feature in featureview.features %}
  IF(event_timestamp >= {{ featureview.name }}_feature_timestamp {% if featureview.ttl == 0 %}{% else %}AND Timestamp_sub(event_timestamp, interval {{ featureview.ttl }} second) < {{ featureview.name }}_feature_timestamp{% endif %}, {{ featureview.name }}__{{ feature }}, NULL) as {{ featureview.name }}__{{ feature }}{% if loop.last %}{% else %}, {% endif %}
  {% endfor %}
FROM (
SELECT
  row_number,
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
 4. Finally, deduplicate the rows by selecting the first occurrence of each entity table row_number.
 */
{{ featureview.name }}__deduped AS (SELECT
  k.*
FROM (
  SELECT ARRAY_AGG(row LIMIT 1)[OFFSET(0)] k
  FROM {{ featureview.name }}__joined row
  GROUP BY row_number
)){% if loop.last %}{% else %}, {% endif %}

{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 */
SELECT edf.event_timestamp as event_timestamp, * EXCEPT (row_number, event_timestamp) FROM entity_dataframe edf
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
    row_number,
    {% for feature in featureview.features %}
    {{ featureview.name }}__{{ feature }}{% if loop.last %}{% else %}, {% endif %}
    {% endfor %}
    FROM {{ featureview.name }}__deduped
) USING (row_number)
{% endfor %}
ORDER BY event_timestamp
"""


def get_offline_store(config: RepoConfig) -> Type[OfflineStore]:
    if config.provider == "gcp":
        return BigQueryOfflineStore
    elif config.provider == "local":
        return FileOfflineStore
    else:
        raise ValueError(config)
