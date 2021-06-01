import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Union

import pandas
import pyarrow
from jinja2 import BaseLoader, Environment

from feast.data_source import BigQuerySource, DataSource
from feast.errors import FeastProviderLoginError
from feast.feature_view import FeatureView
from feast.infra.offline_stores.offline_store import OfflineStore
from feast.infra.provider import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
    RetrievalJob,
    _get_requested_feature_views_to_features_dict,
)
from feast.registry import Registry
from feast.repo_config import BigQueryOfflineStoreConfig, RepoConfig

try:
    from google.api_core.exceptions import NotFound
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud import bigquery

except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("gcp", str(e))


class BigQueryOfflineStore(OfflineStore):
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
        assert isinstance(data_source, BigQuerySource)
        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [event_timestamp_column]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamps) + " DESC"
        field_string = ", ".join(join_key_columns + feature_name_columns + timestamps)

        query = f"""
            SELECT {field_string}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression}
                WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
            )
            WHERE _feast_row = 1
            """

        return BigQueryOfflineStore._pull_query(query)

    @staticmethod
    def _pull_query(query: str) -> pyarrow.Table:
        client = _get_bigquery_client()
        query_job = client.query(query)
        return query_job.to_arrow()

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
    ) -> RetrievalJob:
        # TODO: Add entity_df validation in order to fail before interacting with BigQuery

        client = _get_bigquery_client()

        if type(entity_df) is str:
            entity_df_job = client.query(entity_df)
            entity_df_result = entity_df_job.result()  # also starts job

            entity_df_event_timestamp_col = _infer_event_timestamp_from_bigquery_query(
                entity_df_result
            )

            entity_df_sql_table = f"`{entity_df_job.destination.project}.{entity_df_job.destination.dataset_id}.{entity_df_job.destination.table_id}`"
        elif isinstance(entity_df, pandas.DataFrame):
            entity_df_event_timestamp_col = _infer_event_timestamp_from_dataframe(
                entity_df
            )

            assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)

            table_id = _upload_entity_df_into_bigquery(
                config.project, config.offline_store.dataset, entity_df, client
            )
            entity_df_sql_table = f"`{table_id}`"
        else:
            raise ValueError(
                f"The entity dataframe you have provided must be a Pandas DataFrame or BigQuery SQL query, "
                f"but we found: {type(entity_df)} "
            )

        # Build a query context containing all information required to template the BigQuery SQL query
        query_context = get_feature_view_query_context(
            feature_refs, feature_views, registry, project
        )

        # TODO: Infer min_timestamp and max_timestamp from entity_df
        # Generate the BigQuery SQL query from the query context
        query = build_point_in_time_query(
            query_context,
            min_timestamp=datetime.now() - timedelta(days=365),
            max_timestamp=datetime.now() + timedelta(days=1),
            left_table_query_string=entity_df_sql_table,
            entity_df_event_timestamp_col=entity_df_event_timestamp_col,
        )

        job = BigQueryRetrievalJob(query=query, client=client)
        return job


def _infer_event_timestamp_from_bigquery_query(entity_df_result) -> str:
    if any(
        schema_field.name == DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
        for schema_field in entity_df_result.schema
    ):
        return DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
    else:
        datetime_columns = list(
            filter(
                lambda schema_field: schema_field.field_type == "TIMESTAMP",
                entity_df_result.schema,
            )
        )
        if len(datetime_columns) == 1:
            print(
                f"Using {datetime_columns[0].name} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
            )
            return datetime_columns[0].name
        else:
            raise ValueError(
                f"Please provide an entity_df with a column named {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL} representing the time of events."
            )


def _infer_event_timestamp_from_dataframe(entity_df: pandas.DataFrame) -> str:
    if DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL in entity_df.columns:
        return DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL
    else:
        datetime_columns = entity_df.select_dtypes(
            include=["datetime", "datetimetz"]
        ).columns
        if len(datetime_columns) == 1:
            print(
                f"Using {datetime_columns[0]} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
            )
            return datetime_columns[0]
        else:
            raise ValueError(
                f"Please provide an entity_df with a column named {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL} representing the time of events."
            )


class BigQueryRetrievalJob(RetrievalJob):
    def __init__(self, query, client):
        self.query = query
        self.client = client

    def to_df(self):
        # TODO: Ideally only start this job when the user runs "get_historical_features", not when they run to_df()
        df = self.client.query(self.query).to_dataframe(create_bqstorage_client=True)
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
    created_timestamp_column: Optional[str]
    query: str
    table_subquery: str
    entity_selections: List[str]


def _upload_entity_df_into_bigquery(project, dataset_name, entity_df, client) -> str:
    """Uploads a Pandas entity dataframe into a BigQuery table and returns a reference to the resulting table"""

    # First create the BigQuery dataset if it doesn't exist
    dataset = bigquery.Dataset(f"{client.project}.{dataset_name}")
    dataset.location = "US"

    try:
        client.get_dataset(dataset)
    except NotFound:
        # Only create the dataset if it does not exist
        client.create_dataset(dataset, exists_ok=True)

    # Drop the index so that we dont have unnecessary columns
    entity_df.reset_index(drop=True, inplace=True)

    # Upload the dataframe into BigQuery, creating a temporary table
    job_config = bigquery.LoadJobConfig()
    table_id = f"{client.project}.{dataset_name}.entity_df_{project}_{int(time.time())}"
    job = client.load_table_from_dataframe(entity_df, table_id, job_config=job_config,)
    job.result()

    # Ensure that the table expires after some time
    table = client.get_table(table=table_id)
    table.expires = datetime.utcnow() + timedelta(minutes=30)
    client.update_table(table, ["expires"])

    return table_id


def get_feature_view_query_context(
    feature_refs: List[str],
    feature_views: List[FeatureView],
    registry: Registry,
    project: str,
) -> List[FeatureViewQueryContext]:
    """Build a query context containing all information required to template a BigQuery point-in-time SQL query"""

    feature_views_to_feature_map = _get_requested_feature_views_to_features_dict(
        feature_refs, feature_views
    )

    query_context = []
    for feature_view, features in feature_views_to_feature_map.items():
        join_keys = []
        entity_selections = []
        reverse_field_mapping = {
            v: k for k, v in feature_view.input.field_mapping.items()
        }
        for entity_name in feature_view.entities:
            entity = registry.get_entity(entity_name, project)
            join_keys.append(entity.join_key)
            join_key_column = reverse_field_mapping.get(
                entity.join_key, entity.join_key
            )
            entity_selections.append(f"{join_key_column} AS {entity.join_key}")

        if isinstance(feature_view.ttl, timedelta):
            ttl_seconds = int(feature_view.ttl.total_seconds())
        else:
            ttl_seconds = 0

        assert isinstance(feature_view.input, BigQuerySource)

        event_timestamp_column = feature_view.input.event_timestamp_column
        created_timestamp_column = feature_view.input.created_timestamp_column

        context = FeatureViewQueryContext(
            name=feature_view.name,
            ttl=ttl_seconds,
            entities=join_keys,
            features=features,
            table_ref=feature_view.input.table_ref,
            event_timestamp_column=reverse_field_mapping.get(
                event_timestamp_column, event_timestamp_column
            ),
            created_timestamp_column=reverse_field_mapping.get(
                created_timestamp_column, created_timestamp_column
            ),
            # TODO: Make created column optional and not hardcoded
            query=feature_view.input.query,
            table_subquery=feature_view.input.get_table_query_string(),
            entity_selections=entity_selections,
        )
        query_context.append(context)
    return query_context


def build_point_in_time_query(
    feature_view_query_contexts: List[FeatureViewQueryContext],
    min_timestamp: datetime,
    max_timestamp: datetime,
    left_table_query_string: str,
    entity_df_event_timestamp_col: str,
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
        "entity_df_event_timestamp_col": entity_df_event_timestamp_col,
        "unique_entity_keys": set(
            [entity for fv in feature_view_query_contexts for entity in fv.entities]
        ),
        "featureviews": [asdict(context) for context in feature_view_query_contexts],
    }

    query = template.render(template_context)
    return query


def _get_bigquery_client():
    try:
        client = bigquery.Client()
    except DefaultCredentialsError as e:
        raise FeastProviderLoginError(
            str(e)
            + '\nIt may be necessary to run "gcloud auth application-default login" if you would like to use your '
            "local Google Cloud account"
        )
    except EnvironmentError as e:
        raise FeastProviderLoginError(
            "GCP error: "
            + str(e)
            + "\nIt may be necessary to set a default GCP project by running "
            '"gcloud config set project your-project"'
        )

    return client


# TODO: Optimizations
#   * Use GENERATE_UUID() instead of ROW_NUMBER(), or join on entity columns directly
#   * Precompute ROW_NUMBER() so that it doesn't have to be recomputed for every query on entity_dataframe
#   * Create temporary tables instead of keeping all tables in memory

SINGLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
WITH entity_dataframe AS (
    SELECT
        *,
        CONCAT(
            {% for entity_key in unique_entity_keys %}
                CAST({{entity_key}} AS STRING),
            {% endfor %}
            CAST({{entity_df_event_timestamp_col}} AS STRING)
        ) AS entity_row_unique_id
    FROM {{ left_table_query_string }}
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
  entity_row_unique_id,
  -- event_timestamp contains the timestamps to join onto
  {{entity_df_event_timestamp_col}} AS event_timestamp,
  -- the feature_timestamp, i.e. the latest occurrence of the requested feature relative to the entity_dataset timestamp
  NULL as {{ featureview.name }}_feature_timestamp,
  -- created timestamp of the feature at the corresponding feature_timestamp
  {{ 'NULL as created_timestamp,' if featureview.created_timestamp_column else '' }}
  -- select only entities belonging to this feature set
  {{ featureview.entities | join(', ')}},
  -- boolean for filtering the dataset later
  true AS is_entity_table
FROM entity_dataframe
UNION ALL
SELECT
  NULL as entity_row_unique_id,
  {{ featureview.event_timestamp_column }} as event_timestamp,
  {{ featureview.event_timestamp_column }} as {{ featureview.name }}_feature_timestamp,
  {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
  {{ featureview.entity_selections | join(', ')}},
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
  entity_row_unique_id,
  event_timestamp,
  {{ featureview.entities | join(', ')}},
  {% for feature in featureview.features %}
  IF(event_timestamp >= {{ featureview.name }}_feature_timestamp {% if featureview.ttl == 0 %}{% else %}AND Timestamp_sub(event_timestamp, interval {{ featureview.ttl }} second) < {{ featureview.name }}_feature_timestamp{% endif %}, {{ featureview.name }}__{{ feature }}, NULL) as {{ featureview.name }}__{{ feature }}{% if loop.last %}{% else %}, {% endif %}
  {% endfor %}
FROM (
SELECT
  entity_row_unique_id,
  event_timestamp,
  {{ featureview.entities | join(', ')}},
  {{ 'FIRST_VALUE(created_timestamp IGNORE NULLS) over w AS created_timestamp,' if featureview.created_timestamp_column else '' }}
  FIRST_VALUE({{ featureview.name }}_feature_timestamp IGNORE NULLS) over w AS {{ featureview.name }}_feature_timestamp,
  is_entity_table
FROM {{ featureview.name }}__union_features
WINDOW w AS (PARTITION BY {{ featureview.entities | join(', ') }} ORDER BY event_timestamp DESC, is_entity_table DESC{{', created_timestamp DESC' if featureview.created_timestamp_column else ''}} ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
)
/*
 3. Select only the rows from the entity table, and join the features from the original feature set table
 to the dataset using the entity values, feature_timestamp, and created_timestamps.
 */
LEFT JOIN (
SELECT
  {{ featureview.event_timestamp_column }} as {{ featureview.name }}_feature_timestamp,
  {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
  {{ featureview.entity_selections | join(', ')}},
  {% for feature in featureview.features %}
  {{ feature }} as {{ featureview.name }}__{{ feature }}{% if loop.last %}{% else %}, {% endif %}
  {% endfor %}
FROM {{ featureview.table_subquery }} WHERE {{ featureview.event_timestamp_column }} <= '{{ max_timestamp }}'
{% if featureview.ttl == 0 %}{% else %}AND {{ featureview.event_timestamp_column }} >= Timestamp_sub(TIMESTAMP '{{ min_timestamp }}', interval {{ featureview.ttl }} second){% endif %}
) USING ({{ featureview.name }}_feature_timestamp,{{ ' created_timestamp,' if featureview.created_timestamp_column else '' }} {{ featureview.entities | join(', ')}})
WHERE is_entity_table
),
/*
 4. Finally, deduplicate the rows by selecting the first occurrence of each entity table entity_row_unique_id.
 */
{{ featureview.name }}__deduped AS (SELECT
  k.*
FROM (
  SELECT ARRAY_AGG(row LIMIT 1)[OFFSET(0)] k
  FROM {{ featureview.name }}__joined row
  GROUP BY entity_row_unique_id
)){% if loop.last %}{% else %}, {% endif %}

{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 */
SELECT edf.{{entity_df_event_timestamp_col}} as {{entity_df_event_timestamp_col}}, * EXCEPT (entity_row_unique_id, {{entity_df_event_timestamp_col}}) FROM entity_dataframe edf
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
    entity_row_unique_id,
    {% for feature in featureview.features %}
    {{ featureview.name }}__{{ feature }}{% if loop.last %}{% else %}, {% endif %}
    {% endfor %}
    FROM {{ featureview.name }}__deduped
) USING (entity_row_unique_id)
{% endfor %}
ORDER BY {{entity_df_event_timestamp_col}}
"""
