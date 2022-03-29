import warnings
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

import numpy as np
import pandas
import pandas as pd
import pyarrow
import pyspark
from pydantic import StrictStr
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pytz import utc

from feast import FeatureView, OnDemandFeatureView
from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import spark_schema_to_np_dtypes
from feast.usage import log_exceptions_and_usage


class SparkOfflineStoreConfig(FeastConfigBaseModel):
    type: StrictStr = "spark"
    """ Offline store type selector"""

    spark_conf: Optional[Dict[str, str]] = None
    """ Configuration overlay for the spark session """
    # sparksession is not serializable and we dont want to pass it around as an argument


class SparkOfflineStore(OfflineStore):
    @staticmethod
    @log_exceptions_and_usage(offline_store="spark")
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
        spark_session = get_spark_session_or_start_new_with_repoconfig(
            config.offline_store
        )
        assert isinstance(config.offline_store, SparkOfflineStoreConfig)
        assert isinstance(data_source, SparkSource)

        warnings.warn(
            "The spark offline store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )

        print("Pulling latest features from spark offline store")

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

        start_date_str = _format_datetime(start_date)
        end_date_str = _format_datetime(end_date)
        query = f"""
                SELECT
                    {field_string}
                    {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
                FROM (
                    SELECT {field_string},
                    ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS feast_row_
                    FROM {from_expression} t1
                    WHERE {event_timestamp_column} BETWEEN TIMESTAMP('{start_date_str}') AND TIMESTAMP('{end_date_str}')
                ) t2
                WHERE feast_row_ = 1
                """

        return SparkRetrievalJob(
            spark_session=spark_session,
            query=query,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="spark")
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pandas.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, SparkOfflineStoreConfig)
        warnings.warn(
            "The spark offline store is an experimental feature in alpha development. "
            "Some functionality may still be unstable so functionality can change in the future.",
            RuntimeWarning,
        )
        spark_session = get_spark_session_or_start_new_with_repoconfig(
            store_config=config.offline_store
        )
        tmp_entity_df_table_name = offline_utils.get_temp_entity_table_name()

        entity_schema = _upload_entity_df_and_get_entity_schema(
            spark_session=spark_session,
            table_name=tmp_entity_df_table_name,
            entity_df=entity_df,
        )
        event_timestamp_col = offline_utils.infer_event_timestamp_from_entity_df(
            entity_schema=entity_schema,
        )
        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df, event_timestamp_col, spark_session,
        )

        expected_join_keys = offline_utils.get_expected_join_keys(
            project=project, feature_views=feature_views, registry=registry
        )
        offline_utils.assert_expected_columns_in_entity_df(
            entity_schema=entity_schema,
            join_keys=expected_join_keys,
            entity_df_event_timestamp_col=event_timestamp_col,
        )

        query_context = offline_utils.get_feature_view_query_context(
            feature_refs,
            feature_views,
            registry,
            project,
            entity_df_event_timestamp_range,
        )

        query = offline_utils.build_point_in_time_query(
            feature_view_query_contexts=query_context,
            left_table_query_string=tmp_entity_df_table_name,
            entity_df_event_timestamp_col=event_timestamp_col,
            entity_df_columns=entity_schema.keys(),
            query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
            full_feature_names=full_feature_names,
        )

        return SparkRetrievalJob(
            spark_session=spark_session,
            query=query,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(set(entity_schema.keys()) - {event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="spark")
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        """
        Note that join_key_columns, feature_name_columns, event_timestamp_column, and created_timestamp_column
        have all already been mapped to column names of the source table and those column names are the values passed
        into this function.
        """
        assert isinstance(data_source, SparkSource)
        warnings.warn(
            "The spark offline store is an experimental feature in alpha development. "
            "This API is unstable and it could and most probably will be changed in the future.",
            RuntimeWarning,
        )
        from_expression = data_source.get_table_query_string()

        field_string = (
            '"'
            + '", "'.join(
                join_key_columns + feature_name_columns + [event_timestamp_column]
            )
            + '"'
        )
        start_date = start_date.astimezone(tz=utc)
        end_date = end_date.astimezone(tz=utc)

        query = f"""
            SELECT {field_string}
            FROM {from_expression}
            WHERE "{event_timestamp_column}" BETWEEN TIMESTAMP '{start_date}' AND TIMESTAMP '{end_date}'
        """
        spark_session = get_spark_session_or_start_new_with_repoconfig(
            store_config=config.offline_store
        )
        return SparkRetrievalJob(
            spark_session=spark_session, query=query, full_feature_names=False
        )


class SparkRetrievalJob(RetrievalJob):
    def __init__(
        self,
        spark_session: SparkSession,
        query: str,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        super().__init__()
        self.spark_session = spark_session
        self.query = query
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> Optional[List[OnDemandFeatureView]]:
        return self._on_demand_feature_views

    def to_spark_df(self) -> pyspark.sql.DataFrame:
        statements = self.query.split(
            "---EOS---"
        )  # TODO can do better than this dirty split
        *_, last = map(self.spark_session.sql, statements)
        return last

    def _to_df_internal(self) -> pd.DataFrame:
        """Return dataset as Pandas DataFrame synchronously"""
        return self.to_spark_df().toPandas()

    def _to_arrow_internal(self) -> pyarrow.Table:
        """Return dataset as pyarrow Table synchronously"""
        df = self.to_df()
        return pyarrow.Table.from_pandas(df)  # noqa

    def persist(self, storage: SavedDatasetStorage):
        """
        Run the retrieval and persist the results in the same offline store used for read.
        """
        pass

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        """
        Return metadata information about retrieval.
        Should be available even before materializing the dataset itself.
        """
        return self._metadata


def get_spark_session_or_start_new_with_repoconfig(
    store_config: SparkOfflineStoreConfig,
) -> SparkSession:
    spark_session = SparkSession.getActiveSession()
    if not spark_session:
        spark_builder = SparkSession.builder
        spark_conf = store_config.spark_conf
        if spark_conf:
            spark_builder = spark_builder.config(
                conf=SparkConf().setAll([(k, v) for k, v in spark_conf.items()])
            )

        spark_session = spark_builder.getOrCreate()
    spark_session.conf.set("spark.sql.parser.quotedRegexColumnNames", "true")
    return spark_session


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    spark_session: SparkSession,
) -> Tuple[datetime, datetime]:
    if isinstance(entity_df, pd.DataFrame):
        entity_df_event_timestamp = entity_df.loc[
            :, entity_df_event_timestamp_col
        ].infer_objects()
        if pd.api.types.is_string_dtype(entity_df_event_timestamp):
            entity_df_event_timestamp = pd.to_datetime(
                entity_df_event_timestamp, utc=True
            )
        entity_df_event_timestamp_range = (
            entity_df_event_timestamp.min().to_pydatetime(),
            entity_df_event_timestamp.max().to_pydatetime(),
        )
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), determine range
        # from table
        df = spark_session.sql(entity_df).select(entity_df_event_timestamp_col)
        # TODO(kzhang132): need utc conversion here.
        entity_df_event_timestamp_range = (
            df.agg({entity_df_event_timestamp_col: "max"}).collect()[0][0],
            df.agg({entity_df_event_timestamp_col: "min"}).collect()[0][0],
        )
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


def _upload_entity_df_and_get_entity_schema(
    spark_session: SparkSession,
    table_name: str,
    entity_df: Union[pandas.DataFrame, str],
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        spark_session.createDataFrame(entity_df).createOrReplaceTempView(table_name)
        return dict(zip(entity_df.columns, entity_df.dtypes))
    elif isinstance(entity_df, str):
        spark_session.sql(entity_df).createOrReplaceTempView(table_name)
        limited_entity_df = spark_session.table(table_name)
        return dict(
            zip(
                limited_entity_df.columns,
                spark_schema_to_np_dtypes(limited_entity_df.dtypes),
            )
        )
    else:
        raise InvalidEntityType(type(entity_df))


def _format_datetime(t: datetime) -> str:
    # Since Hive does not support timezone, need to transform to utc.
    if t.tzinfo:
        t = t.astimezone(tz=utc)
    dt = t.strftime("%Y-%m-%d %H:%M:%S.%f")
    return dt


MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
CREATE OR REPLACE TEMPORARY VIEW entity_dataframe AS (
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
);
---EOS---
-- Start create temporary table *__base
{% for featureview in featureviews %}
CREATE OR REPLACE TEMPORARY VIEW {{ featureview.name }}__base AS
WITH {{ featureview.name }}__entity_dataframe AS (
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
    FROM {{ featureview.table_subquery }} AS subquery
    INNER JOIN (
        SELECT MAX(entity_timestamp) as max_entity_timestamp_
               {% if featureview.ttl == 0 %}{% else %}
               ,(MIN(entity_timestamp) - interval '{{ featureview.ttl }}' second) as min_entity_timestamp_
               {% endif %}
        FROM entity_dataframe
    ) AS temp
    ON (
        {{ featureview.event_timestamp_column }} <= max_entity_timestamp_
        {% if featureview.ttl == 0 %}{% else %}
        AND {{ featureview.event_timestamp_column }} >=  min_entity_timestamp_
        {% endif %}
    )
)
SELECT
    subquery.*,
    entity_dataframe.entity_timestamp,
    entity_dataframe.{{featureview.name}}__entity_row_unique_id
FROM {{ featureview.name }}__subquery AS subquery
INNER JOIN (
    SELECT *
    {% if featureview.ttl == 0 %}{% else %}
    , (entity_timestamp - interval '{{ featureview.ttl }}' second) as ttl_entity_timestamp
    {% endif %}
    FROM {{ featureview.name }}__entity_dataframe
) AS entity_dataframe
ON (
    subquery.event_timestamp <= entity_dataframe.entity_timestamp
    {% if featureview.ttl == 0 %}{% else %}
    AND subquery.event_timestamp >= entity_dataframe.ttl_entity_timestamp
    {% endif %}
    {% for entity in featureview.entities %}
    AND subquery.{{ entity }} = entity_dataframe.{{ entity }}
    {% endfor %}
);
---EOS---
{% endfor %}
-- End create temporary table *__base
{% for featureview in featureviews %}
{% if loop.first %}WITH{% endif %}
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
        base.{{featureview.name}}__entity_row_unique_id,
        MAX(base.event_timestamp) AS event_timestamp
        {% if featureview.created_timestamp_column %}
            ,MAX(base.created_timestamp) AS created_timestamp
        {% endif %}
    FROM {{ featureview.name }}__base AS base
    {% if featureview.created_timestamp_column %}
        INNER JOIN {{ featureview.name }}__dedup AS dedup
        ON (
            dedup.{{featureview.name}}__entity_row_unique_id=base.{{featureview.name}}__entity_row_unique_id
            AND dedup.event_timestamp=base.event_timestamp
            AND dedup.created_timestamp=base.created_timestamp
        )
    {% endif %}
    GROUP BY base.{{featureview.name}}__entity_row_unique_id
),
/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
{{ featureview.name }}__cleaned AS (
    SELECT base.*
    FROM {{ featureview.name }}__base AS base
    INNER JOIN {{ featureview.name }}__latest AS latest
    ON (
        base.{{featureview.name}}__entity_row_unique_id=latest.{{featureview.name}}__entity_row_unique_id
        AND base.event_timestamp=latest.event_timestamp
        {% if featureview.created_timestamp_column %}
            AND base.created_timestamp=latest.created_timestamp
        {% endif %}
    )
){% if loop.last %}{% else %}, {% endif %}
{% endfor %}
/*
 Joins the outputs of multiple time travel joins to a single table.
 The entity_dataframe dataset being our source of truth here.
 */
SELECT `(entity_timestamp|{% for featureview in featureviews %}{{featureview.name}}__entity_row_unique_id{% if loop.last %}{% else %}|{% endif %}{% endfor %})?+.+`
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        {{featureview.name}}__entity_row_unique_id
        {% for feature in featureview.features %}
            ,{% if full_feature_names %}{{ featureview.name }}__{{feature}}{% else %}{{ feature }}{% endif %}
        {% endfor %}
    FROM {{ featureview.name }}__cleaned
) AS {{ featureview.name }}__joined
ON (
    {{ featureview.name }}__joined.{{featureview.name}}__entity_row_unique_id=entity_dataframe.{{featureview.name}}__entity_row_unique_id
)
{% endfor %}"""
