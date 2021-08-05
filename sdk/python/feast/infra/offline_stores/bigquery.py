import uuid
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Union

import numpy as np
import pandas
import pyarrow
from pydantic import StrictStr
from pydantic.typing import Literal
from tenacity import Retrying, retry_if_exception_type, stop_after_delay, wait_fixed

from feast.data_source import DataSource
from feast.errors import (
    BigQueryJobCancelled,
    BigQueryJobStillRunning,
    FeastProviderLoginError,
    InvalidEntityType,
)
from feast.feature_view import FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig

from .bigquery_source import BigQuerySource

try:
    from google.api_core.exceptions import NotFound
    from google.auth.exceptions import DefaultCredentialsError
    from google.cloud import bigquery
    from google.cloud.bigquery import Client

except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("gcp", str(e))


class BigQueryOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for GCP BigQuery """

    type: Literal["bigquery"] = "bigquery"
    """ Offline store type selector"""

    dataset: StrictStr = "feast"
    """ (optional) BigQuery Dataset name for temporary tables """

    project_id: Optional[StrictStr] = None
    """ (optional) GCP project name used for the BigQuery offline store """


class BigQueryOfflineStore(OfflineStore):
    @staticmethod
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

        client = _get_bigquery_client(project=config.offline_store.project_id)
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
        return BigQueryRetrievalJob(query=query, client=client, config=config)

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        # TODO: Add entity_df validation in order to fail before interacting with BigQuery
        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)

        client = _get_bigquery_client(project=config.offline_store.project_id)

        assert isinstance(config.offline_store, BigQueryOfflineStoreConfig)

        table_reference = _get_table_reference_for_new_entity(
            client, client.project, config.offline_store.dataset
        )

        entity_schema = _upload_entity_df_and_get_entity_schema(
            client=client, table_name=table_reference, entity_df=entity_df,
        )

        entity_df_event_timestamp_col = offline_utils.infer_event_timestamp_from_entity_df(
            entity_schema
        )

        expected_join_keys = offline_utils.get_expected_join_keys(
            project, feature_views, registry
        )

        offline_utils.assert_expected_columns_in_entity_df(
            entity_schema, expected_join_keys, entity_df_event_timestamp_col
        )

        # Build a query context containing all information required to template the BigQuery SQL query
        query_context = offline_utils.get_feature_view_query_context(
            feature_refs, feature_views, registry, project,
        )

        # Generate the BigQuery SQL query from the query context
        query = offline_utils.build_point_in_time_query(
            query_context,
            left_table_query_string=table_reference,
            entity_df_event_timestamp_col=entity_df_event_timestamp_col,
            query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
            full_feature_names=full_feature_names,
        )

        return BigQueryRetrievalJob(query=query, client=client, config=config)


class BigQueryRetrievalJob(RetrievalJob):
    def __init__(self, query, client, config):
        self.query = query
        self.client = client
        self.config = config

    def to_df(self):
        # TODO: Ideally only start this job when the user runs "get_historical_features", not when they run to_df()
        df = self.client.query(self.query).to_dataframe(create_bqstorage_client=True)
        return df

    def to_sql(self) -> str:
        """
        Returns the SQL query that will be executed in BigQuery to build the historical feature table.
        """
        return self.query

    def to_bigquery(
        self,
        job_config: bigquery.QueryJobConfig = None,
        timeout: int = 1800,
        retry_cadence: int = 10,
    ) -> Optional[str]:
        """
        Triggers the execution of a historical feature retrieval query and exports the results to a BigQuery table.
        Runs for a maximum amount of time specified by the timeout parameter (defaulting to 30 minutes).

        Args:
            job_config: An optional bigquery.QueryJobConfig to specify options like destination table, dry run, etc.
            timeout: An optional number of seconds for setting the time limit of the QueryJob.
            retry_cadence: An optional number of seconds for setting how long the job should checked for completion.

        Returns:
            Returns the destination table name or returns None if job_config.dry_run is True.
        """

        if not job_config:
            today = date.today().strftime("%Y%m%d")
            rand_id = str(uuid.uuid4())[:7]
            path = f"{self.client.project}.{self.config.offline_store.dataset}.historical_{today}_{rand_id}"
            job_config = bigquery.QueryJobConfig(destination=path)

        bq_job = self.client.query(self.query, job_config=job_config)

        if job_config.dry_run:
            print(
                "This query will process {} bytes.".format(bq_job.total_bytes_processed)
            )
            return None

        block_until_done(client=self.client, bq_job=bq_job, timeout=timeout)

        print(f"Done writing to '{job_config.destination}'.")
        return str(job_config.destination)

    def to_arrow(self) -> pyarrow.Table:
        return self.client.query(self.query).to_arrow()


def block_until_done(
    client: Client,
    bq_job: Union[bigquery.job.query.QueryJob, bigquery.job.load.LoadJob],
    timeout: int = 1800,
    retry_cadence: int = 10,
):
    """
    Waits for bq_job to finish running, up to a maximum amount of time specified by the timeout parameter (defaulting to 30 minutes).

    Args:
        client: A bigquery.client.Client to monitor the bq_job.
        bq_job: The bigquery.job.QueryJob that blocks until done runnning.
        timeout: An optional number of seconds for setting the time limit of the job.
        retry_cadence: An optional number of seconds for setting how long the job should checked for completion.

    Raises:
        BigQueryJobStillRunning exception if the function has blocked longer than 30 minutes.
        BigQueryJobCancelled exception to signify when that the job has been cancelled (i.e. from timeout or KeyboardInterrupt).
    """

    def _wait_until_done(job_id):
        if client.get_job(job_id).state in ["PENDING", "RUNNING"]:
            raise BigQueryJobStillRunning(job_id=job_id)

    job_id = bq_job.job_id
    try:
        retryer = Retrying(
            wait=wait_fixed(retry_cadence),
            stop=stop_after_delay(timeout),
            retry=retry_if_exception_type(BigQueryJobStillRunning),
            reraise=True,
        )
        retryer(_wait_until_done, job_id)

    finally:
        if client.get_job(job_id).state in ["PENDING", "RUNNING"]:
            client.cancel_job(job_id)
            raise BigQueryJobCancelled(job_id=job_id)

        if bq_job.exception():
            raise bq_job.exception()


def _get_table_reference_for_new_entity(
    client: Client, dataset_project: str, dataset_name: str
) -> str:
    """Gets the table_id for the new entity to be uploaded."""

    # First create the BigQuery dataset if it doesn't exist
    dataset = bigquery.Dataset(f"{dataset_project}.{dataset_name}")
    dataset.location = "US"

    try:
        client.get_dataset(dataset)
    except NotFound:
        # Only create the dataset if it does not exist
        client.create_dataset(dataset, exists_ok=True)

    table_name = offline_utils.get_temp_entity_table_name()

    return f"{dataset_project}.{dataset_name}.{table_name}"


def _upload_entity_df_and_get_entity_schema(
    client: Client, table_name: str, entity_df: Union[pandas.DataFrame, str],
) -> Dict[str, np.dtype]:
    """Uploads a Pandas entity dataframe into a BigQuery table and returns the resulting table"""

    if type(entity_df) is str:
        job = client.query(f"CREATE TABLE {table_name} AS ({entity_df})")
        block_until_done(client, job)

        limited_entity_df = (
            client.query(f"SELECT * FROM {table_name} LIMIT 1").result().to_dataframe()
        )
        entity_schema = dict(zip(limited_entity_df.columns, limited_entity_df.dtypes))
    elif isinstance(entity_df, pandas.DataFrame):
        # Drop the index so that we dont have unnecessary columns
        entity_df.reset_index(drop=True, inplace=True)

        # Upload the dataframe into BigQuery, creating a temporary table
        job_config = bigquery.LoadJobConfig()
        job = client.load_table_from_dataframe(
            entity_df, table_name, job_config=job_config
        )
        block_until_done(client, job)

        entity_schema = dict(zip(entity_df.columns, entity_df.dtypes))
    else:
        raise InvalidEntityType(type(entity_df))

    # Ensure that the table expires after some time
    table = client.get_table(table=table_name)
    table.expires = datetime.utcnow() + timedelta(minutes=30)
    client.update_table(table, ["expires"])

    return entity_schema


def _get_bigquery_client(project: Optional[str] = None):
    try:
        client = bigquery.Client(project=project)
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

# Note: Keep this in sync with sdk/python/feast/infra/offline_stores/redshift.py:MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN

MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
WITH entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            ,CONCAT(
                {% for entity in featureview.entities %}
                    CAST({{entity}} AS STRING),
                {% endfor %}
                CAST({{entity_df_event_timestamp_col}} AS STRING)
            ) AS {{featureview.name}}__entity_row_unique_id
        {% endfor %}
    FROM {{ left_table_query_string }}
),

{% for featureview in featureviews %}

{{ featureview.name }}__entity_dataframe AS (
    SELECT
        {{ featureview.entities | join(', ')}},
        entity_timestamp,
        {{featureview.name}}__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY {{ featureview.entities | join(', ')}}, entity_timestamp, {{featureview.name}}__entity_row_unique_id
),

/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.

 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `event_timestamp_column`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `event_timestamp_column`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously

 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
*/

{{ featureview.name }}__subquery AS (
    SELECT
        {{ featureview.event_timestamp_column }} as event_timestamp,
        {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}},
        {% for feature in featureview.features %}
            {{ feature }} as {% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }}
    WHERE {{ featureview.event_timestamp_column }} <= (SELECT MAX(entity_timestamp) FROM entity_dataframe)
    {% if featureview.ttl == 0 %}{% else %}
    AND {{ featureview.event_timestamp_column }} >= Timestamp_sub((SELECT MIN(entity_timestamp) FROM entity_dataframe), interval {{ featureview.ttl }} second)
    {% endif %}
),

{{ featureview.name }}__base AS (
    SELECT
        subquery.*,
        entity_dataframe.entity_timestamp,
        entity_dataframe.{{featureview.name}}__entity_row_unique_id
    FROM {{ featureview.name }}__subquery AS subquery
    INNER JOIN {{ featureview.name }}__entity_dataframe AS entity_dataframe
    ON TRUE
        AND subquery.event_timestamp <= entity_dataframe.entity_timestamp

        {% if featureview.ttl == 0 %}{% else %}
        AND subquery.event_timestamp >= Timestamp_sub(entity_dataframe.entity_timestamp, interval {{ featureview.ttl }} second)
        {% endif %}

        {% for entity in featureview.entities %}
        AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
        {% endfor %}
),

/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
{{ featureview.name }}__dedup AS (
    SELECT
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM {{ featureview.name }}__base
    GROUP BY {{featureview.name}}__entity_row_unique_id, event_timestamp
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
{{ featureview.name }}__latest AS (
    SELECT
        {{featureview.name}}__entity_row_unique_id,
        MAX(event_timestamp) AS event_timestamp
        {% if featureview.created_timestamp_column %}
            ,ANY_VALUE(created_timestamp) AS created_timestamp
        {% endif %}

    FROM {{ featureview.name }}__base
    {% if featureview.created_timestamp_column %}
        INNER JOIN {{ featureview.name }}__dedup
        USING ({{featureview.name}}__entity_row_unique_id, event_timestamp, created_timestamp)
    {% endif %}

    GROUP BY {{featureview.name}}__entity_row_unique_id
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
{{ featureview.name }}__cleaned AS (
    SELECT base.*
    FROM {{ featureview.name }}__base as base
    INNER JOIN {{ featureview.name }}__latest
    USING(
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp
        {% if featureview.created_timestamp_column %}
            ,created_timestamp
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */

SELECT * EXCEPT(entity_timestamp, {% for featureview in featureviews %} {{featureview.name}}__entity_row_unique_id{% if loop.last %}{% else %},{% endif %}{% endfor %})
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) USING ({{featureview.name}}__entity_row_unique_id)
{% endfor %}
"""
