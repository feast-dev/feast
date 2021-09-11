import uuid
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
import pyarrow
from pydantic import StrictStr
from pydantic.typing import Literal
from tenacity import Retrying, retry_if_exception_type, stop_after_delay, wait_fixed

from feast.data_source import DataSource
from feast.errors import (
    FeastProviderLoginError,
    InvalidEntityType,
    MaxcomputeJobCancelled,
    MaxcomputeJobStillRunning,
    MaxcomputeQueryError,
    MaxcomputeUploadError,
)
from feast.feature_view import FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import OfflineStore, RetrievalJob
from feast.infra.utils import aliyun_utils
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig

from .maxcompute_source import MaxcomputeSource

try:
    import odps
    from odps import ODPS, options

    options.sql.use_odps2_extension = True
    options.tunnel.use_instance_tunnel = True
    options.tunnel.limit_instance_tunnel = False

except ImportError as e:
    from feast.errors import FeastExtrasDependencyImportError

    raise FeastExtrasDependencyImportError("aliyun", str(e))


class MaxcomputeOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for Aliyun Maxcompute """

    type: Literal["maxcompute"] = "maxcompute"
    """ Offline store type selector"""

    region: Optional[StrictStr] = None
    """ (optional)Macompute region name"""

    project: StrictStr
    """ Maxcompute project name"""

    access_key: StrictStr
    """ Maxcompute access key"""

    secret_access_key: StrictStr
    """ Maxcompute secret access key"""

    end_point: Optional[StrictStr] = None
    """ (optional)Maxcompute endpoint"""


class MaxcomputeOfflineStore(OfflineStore):
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
        assert isinstance(data_source, MaxcomputeSource)
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

        # client = aliyun_utils.get_maxcompute_client(project=config.offline_store.project)
        client = aliyun_utils.get_maxcompute_client(
            ak=config.offline_store.access_key,
            sk=config.offline_store.secret_access_key,
            project=config.offline_store.project,
            region=config.offline_store.region,
            endpoint=config.offline_store.end_point,
        )

        query = f"""
            SELECT {field_string}
            FROM (
                SELECT {field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression}
                WHERE cast({event_timestamp_column} as TIMESTAMP) BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
            )
            WHERE _feast_row = 1
            """

        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return MaxcomputeRetrievalJob(
            query=query,
            client=client,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, odps.df.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        # TODO: Add entity_df validation in order to fail before interacting with Maxcompute
        assert isinstance(config.offline_store, MaxcomputeOfflineStoreConfig)

        client = aliyun_utils.get_maxcompute_client(
            ak=config.offline_store.access_key,
            sk=config.offline_store.secret_access_key,
            project=config.offline_store.project,
            region=config.offline_store.region,
            endpoint=config.offline_store.end_point,
        )

        assert isinstance(config.offline_store, MaxcomputeOfflineStoreConfig)

        # local pandas data frame need upload
        if isinstance(entity_df, str):
            table_reference = entity_df
        else:
            table_reference = _get_table_reference_for_new_entity(
                client, config.offline_store.project
            )

        entity_schema = _upload_entity_df_and_get_entity_schema(
            client=client, table_name=table_reference, entity_df=entity_df
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

        # Build a query context containing all information required to template the Maxcompute SQL query
        query_context = offline_utils.get_feature_view_query_context(
            feature_refs, feature_views, registry, project
        )

        # Generate the Maxcompute SQL query from the query context
        query = offline_utils.build_point_in_time_query(
            query_context,
            left_table_query_string=table_reference,
            entity_df_event_timestamp_col=entity_df_event_timestamp_col,
            query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
            full_feature_names=full_feature_names,
        )

        return MaxcomputeRetrievalJob(
            query=query,
            client=client,
            config=config,
            full_feature_names=full_feature_names,
            on_demand_feature_views=registry.list_on_demand_feature_views(
                project, allow_cache=True
            ),
        )


class MaxcomputeRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: str,
        client: ODPS,
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]],
    ):
        self.query = query
        self.client = client
        self.config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> Optional[List[OnDemandFeatureView]]:
        return self._on_demand_feature_views

    def to_df_internal(self) -> pd.DataFrame:
        # TODO: Ideally only start this job when the user runs "get_historical_features", not when they run to_df()
        df = self._to_df()
        return df

    def to_sql(self) -> str:
        """
        Returns the SQL query that will be executed in Maxcompute to build the historical feature table.
        """
        return self.query

    def to_maxcompute(self, table_name: str,overwrite:bool = True) -> None:
        """
        Triggers the execution of a historical feature retrieval query and exports the results to a Maxcompute table.
        Args:
            table_name: specify name of destination table

        """

        if overwrite:
            sql = f"DROP TABLE IF EXISTS {table_name}"
            job = self.client.run_sql(sql)
            print("logview url:", job.get_logview_address())
        query = self.query
        sql = f"CREATE TABLE {table_name} LIFECYCLE 1 AS {query}"
        job = self.client.run_sql(sql)
        print("logview url:", job.get_logview_address())
        job.wait_for_success()

    def _to_df(self) -> pd.DataFrame:
        table_reference = _get_table_reference_for_new_entity(
            self.client, self.config.offline_store.project
        )
        query = self.query
        sql = f"CREATE TABLE {table_reference} LIFECYCLE 1 AS {query}"
        job = self.client.run_sql(sql)
        print("logview url:", job.get_logview_address())
        job.wait_for_success()

        table = odps.df.DataFrame(self.client.get_table(table_reference))
        return table.to_pandas()

    def to_arrow(self) -> pyarrow.Table:
        df = self._to_df()
        return pyarrow.Table.from_pandas(df)


def _get_table_reference_for_new_entity(client: ODPS, dataset_project: str) -> str:
    """Gets the table_id for the new entity to be uploaded."""

    table_name = offline_utils.get_temp_entity_table_name()

    return f"{dataset_project}.{table_name}"


def _upload_entity_df_and_get_entity_schema(
    client: ODPS,
    table_name: str,
    entity_df: Union[pd.DataFrame, odps.df.DataFrame, str],
) -> Dict[str, np.dtype]:
    """Uploads a Pandas entity dataframe into a Maxcompute table and returns the resulting table"""

    if type(entity_df) is str:
        limited_entity_df = (
            odps.df.DataFrame(client.get_table(table_name)).limit(1).execute()
        )
        entity_schema = dict(
            zip(limited_entity_df.schema.names, limited_entity_df.schema.types)
        )
    elif isinstance(entity_df, pd.DataFrame):
        # Drop the index so that we dont have unnecessary columns
        entity_df.reset_index(drop=True, inplace=True)

        # Upload the dataframe into Maxcompute, creating a temporary table
        upload_df = odps.df.DataFrame(entity_df)
        try:
            upload_df.persist(table_name, odps=client, lifecycle=1)
        except Exception as e:
            raise MaxcomputeUploadError(e)

        entity_schema = dict(zip(upload_df.dtypes.names, upload_df.dtypes.types))
    elif isinstance(entity_df, odps.df.DataFrame):
        # Just return the Maxcompute schema
        entity_schema = dict(zip(entity_df.dtypes.names, entity_df.dtypes.types))
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_schema


# TODO: Optimizations
#   * Use GENERATE_UUID() instead of ROW_NUMBER(), or join on entity columns directly
#   * Precompute ROW_NUMBER() so that it doesn't have to be recomputed for every query on entity_dataframe
#   * Create temporary tables instead of keeping all tables in memory

# Note: Keep this in sync with sdk/python/feast/infra/offline_stores/redshift.py:MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN

MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
--Compute a deterministic hash for the `left_table_query_string` that will be used throughout
--all the logic as the field to GROUP BY the data
WITH entity_dataframe AS (
    SELECT *,
        CAST({{entity_df_event_timestamp_col}} AS TIMESTAMP) AS entity_timestamp
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
        cast(entity_timestamp as TIMESTAMP),
        {{featureview.name}}__entity_row_unique_id
    FROM entity_dataframe
    GROUP BY {{ featureview.entities | join(', ')}}, entity_timestamp, {{featureview.name}}__entity_row_unique_id
),


-- This query template performs the point-in-time correctness join for a single feature set table
-- to the provided entity table.
--
-- 1. We first join the current feature_view to the entity dataframe that has been passed.
-- This JOIN has the following logic:
--    - For each row of the entity dataframe, only keep the rows where the `event_timestamp_column`
--    is less than the one provided in the entity dataframe
--    - If there a TTL for the current feature_view, also keep the rows where the `event_timestamp_column`
--    is higher the the one provided minus the TTL
--    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
--    computed previously
--
-- The output of this CTE will contain all the necessary information and already filtered out most
-- of the data that is not relevant.

{{ featureview.name }}__subquery AS (
    SELECT
        cast({{ featureview.event_timestamp_column }} as TIMESTAMP) as event_timestamp,
        {{ featureview.created_timestamp_column ~ ' as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}},
        {% for feature in featureview.features %}
            {{ feature }} as {% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }}
    WHERE cast({{ featureview.event_timestamp_column }} as TIMESTAMP) <= (SELECT MAX(entity_timestamp) FROM entity_dataframe)
    {% if featureview.ttl == 0 %}{% else %}
    AND cast({{ featureview.event_timestamp_column }} as TIMESTAMP) >= DATEADD((SELECT MIN(entity_timestamp) FROM entity_dataframe), -{{ featureview.ttl }}, "ss")
    {% endif %}
),

{{ featureview.name }}__base AS (
    SELECT
        /*+ mapjoin({{ featureview.name }}__entity_dataframe)*/ subquery.*,
        entity_dataframe.entity_timestamp,
        entity_dataframe.{{featureview.name}}__entity_row_unique_id
    FROM {{ featureview.name }}__subquery AS subquery
    JOIN {{ featureview.name }}__entity_dataframe AS entity_dataframe
    ON TRUE
        AND subquery.event_timestamp <= entity_dataframe.entity_timestamp

        {% if featureview.ttl == 0 %}{% else %}
        AND subquery.event_timestamp >= DATEADD(entity_dataframe.entity_timestamp, -{{ featureview.ttl }}, "ss")
        {% endif %}

        {% for entity in featureview.entities %}
        AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
        {% endfor %}
),


-- 2. If the `created_timestamp_column` has been set, we need to
-- deduplicate the data first. This is done by calculating the
-- `MAX(created_at_timestamp)` for each event_timestamp.
-- We then join the data on the next CTE

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


-- 3. The data has been filtered during the first CTE "*__base"
-- Thus we only need to compute the latest timestamp of each feature.

{{ featureview.name }}__latest AS (
    SELECT 
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp,
        created_timestamp
    FROM
    (
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY {{featureview.name}}__entity_row_unique_id
                ORDER BY event_timestamp DESC{% if featureview.created_timestamp_column %},created_timestamp DESC{% endif %}
            ) AS row_number
        FROM {{ featureview.name }}__base
        {% if featureview.created_timestamp_column %}
            JOIN {{ featureview.name }}__dedup
            USING ({{featureview.name}}__entity_row_unique_id, event_timestamp, created_timestamp)
        {% endif %}
    )
    WHERE row_number = 1
),


-- 4. Once we know the latest value of each feature for a given timestamp,
-- we can join again the data back to the original "base" dataset

{{ featureview.name }}__cleaned AS (
    SELECT base.*,
        {{featureview.name}}__entity_row_unique_id
    FROM {{ featureview.name }}__base as base
    JOIN {{ featureview.name }}__latest
    USING(
        {{featureview.name}}__entity_row_unique_id,
        event_timestamp
        {% if featureview.created_timestamp_column %}
            ,created_timestamp
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}


{% endfor %}

-- Joins the outputs of multiple time travel joins to a single table.
-- The entity_dataframe dataset being our source of truth here.

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
) {{ featureview.name }}__u USING ({{featureview.name}}__entity_row_unique_id)
{% endfor %}
"""
