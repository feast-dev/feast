import uuid
from datetime import datetime
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
import pyarrow as pa
from pydantic import StrictStr
from pydantic.typing import Literal

from feast import RedshiftSource
from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.feature_view import FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.utils import aws_utils
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig


class RedshiftOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for AWS Redshift """

    type: Literal["redshift"] = "redshift"
    """ Offline store type selector"""

    cluster_id: StrictStr
    """ Redshift cluster identifier """

    region: StrictStr
    """ Redshift cluster's AWS region """

    user: StrictStr
    """ Redshift user name """

    database: StrictStr
    """ Redshift database name """

    s3_staging_location: StrictStr
    """ S3 path for importing & exporting data to Redshift """

    iam_role: StrictStr
    """ IAM Role for Redshift, granting it access to S3 """


class RedshiftOfflineStore(OfflineStore):
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
        assert isinstance(data_source, RedshiftSource)
        assert isinstance(config.offline_store, RedshiftOfflineStoreConfig)

        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(join_key_columns)
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamp_columns = [event_timestamp_column]
        if created_timestamp_column:
            timestamp_columns.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(timestamp_columns) + " DESC"
        field_string = ", ".join(
            join_key_columns + feature_name_columns + timestamp_columns
        )

        redshift_client = aws_utils.get_redshift_data_client(
            config.offline_store.region
        )
        s3_resource = aws_utils.get_s3_resource(config.offline_store.region)

        query = f"""
            SELECT {field_string}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression}
                WHERE {event_timestamp_column} BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
            )
            WHERE _feast_row = 1
            """
        return RedshiftRetrievalJob(
            query=query,
            redshift_client=redshift_client,
            s3_resource=s3_resource,
            config=config,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, RedshiftOfflineStoreConfig)

        redshift_client = aws_utils.get_redshift_data_client(
            config.offline_store.region
        )
        s3_resource = aws_utils.get_s3_resource(config.offline_store.region)

        table_name = offline_utils.get_temp_entity_table_name()

        entity_schema = _upload_entity_df_and_get_entity_schema(
            entity_df, redshift_client, config, s3_resource, table_name
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

        # Build a query context containing all information required to template the Redshift SQL query
        query_context = offline_utils.get_feature_view_query_context(
            feature_refs, feature_views, registry, project,
        )

        # Generate the Redshift SQL query from the query context
        query = offline_utils.build_point_in_time_query(
            query_context,
            left_table_query_string=table_name,
            entity_df_event_timestamp_col=entity_df_event_timestamp_col,
            query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
            full_feature_names=full_feature_names,
        )

        return RedshiftRetrievalJob(
            query=query,
            redshift_client=redshift_client,
            s3_resource=s3_resource,
            config=config,
            drop_columns=["entity_timestamp"]
            + [
                f"{feature_view.name}__entity_row_unique_id"
                for feature_view in feature_views
            ],
        )


class RedshiftRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: str,
        redshift_client,
        s3_resource,
        config: RepoConfig,
        drop_columns: Optional[List[str]] = None,
    ):
        """Initialize RedshiftRetrievalJob object.

        Args:
            query: Redshift SQL query to execute.
            redshift_client: boto3 redshift-data client
            s3_resource: boto3 s3 resource object
            config: Feast repo config
            drop_columns: Optionally a list of columns to drop before unloading to S3.
                          This is a convenient field, since "SELECT ... EXCEPT col" isn't supported in Redshift.
        """
        self.query = query
        self._redshift_client = redshift_client
        self._s3_resource = s3_resource
        self._config = config
        self._s3_path = (
            self._config.offline_store.s3_staging_location
            + "/unload/"
            + str(uuid.uuid4())
        )
        self._drop_columns = drop_columns

    def to_df(self) -> pd.DataFrame:
        return aws_utils.unload_redshift_query_to_df(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            self._s3_resource,
            self._s3_path,
            self._config.offline_store.iam_role,
            self.query,
            self._drop_columns,
        )

    def to_arrow(self) -> pa.Table:
        return aws_utils.unload_redshift_query_to_pa(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            self._s3_resource,
            self._s3_path,
            self._config.offline_store.iam_role,
            self.query,
            self._drop_columns,
        )

    def to_s3(self) -> str:
        """ Export dataset to S3 in Parquet format and return path """
        aws_utils.execute_redshift_query_and_unload_to_s3(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            self._s3_path,
            self._config.offline_store.iam_role,
            self.query,
            self._drop_columns,
        )
        return self._s3_path

    def to_redshift(self, table_name: str) -> None:
        """ Save dataset as a new Redshift table """
        query = f'CREATE TABLE "{table_name}" AS ({self.query});\n'
        if self._drop_columns is not None:
            for column in self._drop_columns:
                query += f"ALTER TABLE {table_name} DROP COLUMN {column};\n"

        aws_utils.execute_redshift_statement(
            self._redshift_client,
            self._config.offline_store.cluster_id,
            self._config.offline_store.database,
            self._config.offline_store.user,
            query,
        )


def _upload_entity_df_and_get_entity_schema(
    entity_df: Union[pd.DataFrame, str],
    redshift_client,
    config: RepoConfig,
    s3_resource,
    table_name: str,
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        # If the entity_df is a pandas dataframe, upload it to Redshift
        # and construct the schema from the original entity_df dataframe
        aws_utils.upload_df_to_redshift(
            redshift_client,
            config.offline_store.cluster_id,
            config.offline_store.database,
            config.offline_store.user,
            s3_resource,
            f"{config.offline_store.s3_staging_location}/entity_df/{table_name}.parquet",
            config.offline_store.iam_role,
            table_name,
            entity_df,
        )
        return dict(zip(entity_df.columns, entity_df.dtypes))
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), create a Redshift table out of it,
        # get pandas dataframe consisting of 1 row (LIMIT 1) and generate the schema out of it
        aws_utils.execute_redshift_statement(
            redshift_client,
            config.offline_store.cluster_id,
            config.offline_store.database,
            config.offline_store.user,
            f"CREATE TABLE {table_name} AS ({entity_df})",
        )
        limited_entity_df = RedshiftRetrievalJob(
            f"SELECT * FROM {table_name} LIMIT 1", redshift_client, s3_resource, config
        ).to_df()
        return dict(zip(limited_entity_df.columns, limited_entity_df.dtypes))
    else:
        raise InvalidEntityType(type(entity_df))


# This query is based on sdk/python/feast/infra/offline_stores/bigquery.py:MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN
# There are couple of changes from BigQuery:
# 1. Use VARCHAR instead of STRING type
# 2. Use "t - x * interval '1' second" instead of "Timestamp_sub(...)"
# 3. Replace `SELECT * EXCEPT (...)` with `SELECT *`, because `EXCEPT` is not supported by Redshift.
#    Instead, we drop the column later after creating the table out of the query.
# We need to keep this query in sync with BigQuery.

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
                    CAST({{entity}} AS VARCHAR),
                {% endfor %}
                CAST({{entity_df_event_timestamp_col}} AS VARCHAR)
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
    AND {{ featureview.event_timestamp_column }} >= (SELECT MIN(entity_timestamp) FROM entity_dataframe) - {{ featureview.ttl }} * interval '1' second
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
        AND subquery.event_timestamp >= entity_dataframe.entity_timestamp - {{ featureview.ttl }} * interval '1' second
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

SELECT *
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
