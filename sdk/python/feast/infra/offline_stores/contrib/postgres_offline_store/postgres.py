import contextlib
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any,
    Callable,
    ContextManager,
    Dict,
    Iterator,
    KeysView,
    List,
    Literal,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd
import pyarrow as pa
from jinja2 import BaseLoader, Environment
from psycopg import sql

from feast.data_source import DataSource
from feast.errors import InvalidEntityType, ZeroColumnQueryResult, ZeroRowsQueryResult
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    SavedDatasetPostgreSQLStorage,
)
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import get_timestamp_filter_sql
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.utils.postgres.connection_utils import (
    _get_conn,
    df_to_postgres_table,
    get_query_schema,
)
from feast.infra.utils.postgres.postgres_config import PostgreSQLConfig
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import pg_type_code_to_arrow
from feast.utils import _utc_now, make_tzaware

from .postgres_source import PostgreSQLSource


class EntitySelectMode(Enum):
    temp_table = "temp_table"
    """ Use a temporary table to store the entity DataFrame or SQL query when querying feature data """
    embed_query = "embed_query"
    """ Use the entity SQL query directly when querying feature data """


class PostgreSQLOfflineStoreConfig(PostgreSQLConfig):
    type: Literal["postgres"] = "postgres"
    entity_select_mode: EntitySelectMode = EntitySelectMode.temp_table


class PostgreSQLOfflineStore(OfflineStore):
    @staticmethod
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, PostgreSQLOfflineStoreConfig)
        assert isinstance(data_source, PostgreSQLSource)
        from_expression = data_source.get_table_query_string()

        partition_by_join_key_string = ", ".join(_append_alias(join_key_columns, "a"))
        if partition_by_join_key_string != "":
            partition_by_join_key_string = (
                "PARTITION BY " + partition_by_join_key_string
            )
        timestamps = [timestamp_field]
        if created_timestamp_column:
            timestamps.append(created_timestamp_column)
        timestamp_desc_string = " DESC, ".join(_append_alias(timestamps, "a")) + " DESC"
        a_field_string = ", ".join(
            _append_alias(join_key_columns + feature_name_columns + timestamps, "a")
        )
        b_field_string = ", ".join(
            _append_alias(join_key_columns + feature_name_columns + timestamps, "b")
        )

        query = f"""
            SELECT
                {b_field_string}
                {f", {repr(DUMMY_ENTITY_VAL)} AS {DUMMY_ENTITY_ID}" if not join_key_columns else ""}
            FROM (
                SELECT {a_field_string},
                ROW_NUMBER() OVER({partition_by_join_key_string} ORDER BY {timestamp_desc_string}) AS _feast_row
                FROM {from_expression} a
                WHERE a."{timestamp_field}" BETWEEN '{start_date}'::timestamptz AND '{end_date}'::timestamptz
            ) b
            WHERE _feast_row = 1
            """

        return PostgreSQLRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )

    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Optional[Union[pd.DataFrame, str]],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
        **kwargs,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, PostgreSQLOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, PostgreSQLSource)
        start_date: Optional[datetime] = kwargs.get("start_date", None)
        end_date: Optional[datetime] = kwargs.get("end_date", None)

        # Handle non-entity retrieval mode
        if entity_df is None:
            # Default to current time if end_date not provided
            if end_date is None:
                end_date = _utc_now()
            else:
                end_date = make_tzaware(end_date)

            # Calculate start_date from TTL if not provided

            if start_date is None:
                # Find the maximum TTL across all feature views to ensure we capture enough data
                max_ttl_seconds = 0
                for fv in feature_views:
                    if fv.ttl and isinstance(fv.ttl, timedelta):
                        ttl_seconds = int(fv.ttl.total_seconds())
                        max_ttl_seconds = max(max_ttl_seconds, ttl_seconds)

                if max_ttl_seconds > 0:
                    # Start from (end_date - max_ttl) to ensure we capture all relevant features
                    start_date = end_date - timedelta(seconds=max_ttl_seconds)
                else:
                    # If no TTL is set, default to 30 days before end_date
                    start_date = end_date - timedelta(days=30)
            else:
                start_date = make_tzaware(start_date)

            entity_df = pd.DataFrame(
                {
                    "event_timestamp": pd.date_range(
                        start=start_date, end=end_date, freq="1s", tz=timezone.utc
                    )[:1]  # Just one row
                }
            )

        entity_schema = _get_entity_schema(entity_df, config)

        entity_df_event_timestamp_col = (
            offline_utils.infer_event_timestamp_from_entity_df(entity_schema)
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df,
            entity_df_event_timestamp_col,
            config,
        )

        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:
            table_name = offline_utils.get_temp_entity_table_name()

            # If using CTE and entity_df is a SQL query, we don't need a table
            use_cte = (
                isinstance(entity_df, str)
                and config.offline_store.entity_select_mode
                == EntitySelectMode.embed_query
            )
            if use_cte:
                left_table_query_string = entity_df
            else:
                left_table_query_string = table_name
                _upload_entity_df(config, entity_df, table_name)

            expected_join_keys = offline_utils.get_expected_join_keys(
                project, feature_views, registry
            )

            offline_utils.assert_expected_columns_in_entity_df(
                entity_schema, expected_join_keys, entity_df_event_timestamp_col
            )

            query_context = offline_utils.get_feature_view_query_context(
                feature_refs,
                feature_views,
                registry,
                project,
                entity_df_event_timestamp_range,
            )

            query_context_dict = [asdict(context) for context in query_context]
            # Hack for query_context.entity_selections to support uppercase in columns
            for context in query_context_dict:
                context["entity_selections"] = [
                    f""""{entity_selection.replace(" AS ", '" AS "')}\""""
                    for entity_selection in context["entity_selections"]
                ]

            try:
                yield build_point_in_time_query(
                    query_context_dict,
                    left_table_query_string=left_table_query_string,
                    entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                    entity_df_columns=entity_schema.keys(),
                    query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                    full_feature_names=full_feature_names,
                    use_cte=use_cte,
                    start_date=start_date,
                    end_date=end_date,
                )
            finally:
                # Only cleanup if we created a table
                if (
                    config.offline_store.entity_select_mode
                    == EntitySelectMode.temp_table
                ):
                    with _get_conn(config.offline_store) as conn, conn.cursor() as cur:
                        cur.execute(
                            sql.SQL(
                                """
                                DROP TABLE IF EXISTS {};
                                """
                            ).format(sql.Identifier(table_name)),
                        )

        return PostgreSQLRetrievalJob(
            query=query_generator,
            config=config,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(entity_schema.keys() - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )

    @staticmethod
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        created_timestamp_column: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, PostgreSQLOfflineStoreConfig)
        assert isinstance(data_source, PostgreSQLSource)
        from_expression = data_source.get_table_query_string()

        timestamp_fields = [timestamp_field]
        if created_timestamp_column:
            timestamp_fields.append(created_timestamp_column)
        field_string = ", ".join(
            _append_alias(
                join_key_columns + feature_name_columns + timestamp_fields,
                "paftoq_alias",
            )
        )

        timestamp_filter = get_timestamp_filter_sql(
            start_date,
            end_date,
            timestamp_field,
            tz=timezone.utc,
            cast_style="timestamptz",
            date_time_separator=" ",  # backwards compatibility but inconsistent with other offline stores
        )

        query = f"""
            SELECT {field_string}
            FROM {from_expression} AS paftoq_alias
            WHERE {timestamp_filter}
        """

        return PostgreSQLRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
        )


class PostgreSQLRetrievalJob(RetrievalJob):
    def __init__(
        self,
        query: Union[str, Callable[[], ContextManager[str]]],
        config: RepoConfig,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        if not isinstance(query, str):
            self._query_generator = query
        else:

            @contextlib.contextmanager
            def query_generator() -> Iterator[str]:
                assert isinstance(query, str)
                yield query

            self._query_generator = query_generator
        self.config = config
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = on_demand_feature_views or []
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> List[OnDemandFeatureView]:
        return self._on_demand_feature_views

    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        # We use arrow format because it gives better control of the table schema
        return self._to_arrow_internal().to_pandas()

    def to_sql(self) -> str:
        with self._query_generator() as query:
            return query

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        with self._query_generator() as query:
            with _get_conn(self.config.offline_store) as conn, conn.cursor() as cur:
                conn.read_only = True
                cur.execute(query)
                if not cur.description:
                    raise ZeroColumnQueryResult(query)
                fields = [
                    (c.name, pg_type_code_to_arrow(c.type_code))
                    for c in cur.description
                ]
                data = cur.fetchall()
                schema = pa.schema(fields)
                # TODO: Fix...
                data_transposed: List[List[Any]] = []
                for col in range(len(fields)):
                    data_transposed.append([])
                    for row in range(len(data)):
                        data_transposed[col].append(data[row][col])

                table = pa.Table.from_arrays(
                    [pa.array(row) for row in data_transposed], schema=schema
                )
                return table

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ):
        assert isinstance(storage, SavedDatasetPostgreSQLStorage)

        df_to_postgres_table(
            config=self.config.offline_store,
            df=self.to_df(),
            table_name=storage.postgres_options._table,
        )


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    config: RepoConfig,
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
        # If the entity_df is a string (SQL query), determine range from table
        with _get_conn(config.offline_store) as conn, conn.cursor() as cur:
            query = f"""
                SELECT
                    MIN({entity_df_event_timestamp_col}) AS min,
                    MAX({entity_df_event_timestamp_col}) AS max
                FROM ({entity_df}) AS tmp_alias
                """
            cur.execute(query)
            res = cur.fetchone()
            if not res:
                raise ZeroRowsQueryResult(query)
            entity_df_event_timestamp_range = (res[0], res[1])
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


def _append_alias(field_names: List[str], alias: str) -> List[str]:
    return [f'{alias}."{field_name}"' for field_name in field_names]


def build_point_in_time_query(
    feature_view_query_contexts: List[dict],
    left_table_query_string: str,
    entity_df_event_timestamp_col: str,
    entity_df_columns: KeysView[str],
    query_template: str,
    full_feature_names: bool = False,
    use_cte: bool = False,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
) -> str:
    """Build point-in-time query between each feature view table and the entity dataframe for PostgreSQL"""
    template = Environment(loader=BaseLoader()).from_string(source=query_template)

    final_output_feature_names = list(entity_df_columns)
    final_output_feature_names.extend(
        [
            (
                f"{fv['name']}__{fv['field_mapping'].get(feature, feature)}"
                if full_feature_names
                else fv["field_mapping"].get(feature, feature)
            )
            for fv in feature_view_query_contexts
            for feature in fv["features"]
        ]
    )

    # Add additional fields to dict
    template_context = {
        "left_table_query_string": left_table_query_string,
        "entity_df_event_timestamp_col": entity_df_event_timestamp_col,
        "unique_entity_keys": set(
            [entity for fv in feature_view_query_contexts for entity in fv["entities"]]
        ),
        "featureviews": feature_view_query_contexts,
        "full_feature_names": full_feature_names,
        "final_output_feature_names": final_output_feature_names,
        "use_cte": use_cte,
        "start_date": start_date,
        "end_date": end_date,
    }

    query = template.render(template_context)
    return query


def _upload_entity_df(
    config: RepoConfig, entity_df: Union[pd.DataFrame, str], table_name: str
):
    if isinstance(entity_df, pd.DataFrame):
        # If the entity_df is a pandas dataframe, upload it to Postgres
        df_to_postgres_table(config.offline_store, entity_df, table_name)
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), create a Postgres table out of it
        with _get_conn(config.offline_store) as conn, conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {table_name} AS ({entity_df})")
    else:
        raise InvalidEntityType(type(entity_df))


def _get_entity_schema(
    entity_df: Union[pd.DataFrame, str],
    config: RepoConfig,
) -> Dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))

    elif isinstance(entity_df, str):
        df_query = f"({entity_df}) AS sub"
        return get_query_schema(config.offline_store, df_query)
    else:
        raise InvalidEntityType(type(entity_df))


# Copied from the Feast Redshift offline store implementation
# Note: Keep this in sync with sdk/python/feast/infra/offline_stores/redshift.py:
# MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN
# https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/offline_stores/redshift.py

MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
{% if start_date and end_date %}
/*
 Non-entity timestamp range query - use JOINs to combine features into single rows
*/
{% if featureviews | length == 1 %}
SELECT
    "{{ featureviews[0].timestamp_field }}" AS event_timestamp,
    {% if featureviews[0].created_timestamp_column %}
    "{{ featureviews[0].created_timestamp_column }}" AS created_timestamp,
    {% endif %}
    {% for entity in featureviews[0].entities %}
    "{{ entity }}",
    {% endfor %}
    {% for feature in featureviews[0].features %}
    "{{ feature }}" AS {% if full_feature_names %}"{{ featureviews[0].name }}__{{ featureviews[0].field_mapping.get(feature, feature) }}"{% else %}"{{ featureviews[0].field_mapping.get(feature, feature) }}"{% endif %}{% if not loop.last %},{% endif %}
    {% endfor %}
    FROM {{ featureviews[0].table_subquery }} as fv_alias
WHERE "{{ featureviews[0].timestamp_field }}" BETWEEN '{{ start_date }}' AND '{{ end_date }}'
{% if featureviews[0].ttl != 0 and featureviews[0].min_event_timestamp %}
AND "{{ featureviews[0].timestamp_field }}" >= '{{ featureviews[0].min_event_timestamp }}'
{% endif %}
{% else %}
WITH
{% for featureview in featureviews %}
"{{ featureview.name }}__data" AS (
    SELECT
        "{{ featureview.timestamp_field }}" AS event_timestamp,
        {% if featureview.created_timestamp_column %}
        "{{ featureview.created_timestamp_column }}" AS created_timestamp,
        {% endif %}
        {% for entity in featureview.entities %}
        "{{ entity }}",
        {% endfor %}
        {% for feature in featureview.features %}
        "{{ feature }}" AS {% if full_feature_names %}"{{ featureview.name }}__{{ featureview.field_mapping.get(feature, feature) }}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }} AS sub
    WHERE "{{ featureview.timestamp_field }}" BETWEEN '{{ start_date }}' AND '{{ end_date }}'
    {% if featureview.ttl != 0 and featureview.min_event_timestamp %}
    AND "{{ featureview.timestamp_field }}" >= '{{ featureview.min_event_timestamp }}'
    {% endif %}
),
{% endfor %}

-- Create a base query with all unique entity + timestamp combinations
base_entities AS (
    {% for featureview in featureviews %}
    SELECT DISTINCT
        event_timestamp,
        {% for entity in featureview.entities %}
        "{{ entity }}"{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM "{{ featureview.name }}__data"
    {% if not loop.last %}
    UNION
    {% endif %}
    {% endfor %}
)

SELECT
    base.event_timestamp,
    {% set all_entities = [] %}
    {% for featureview in featureviews %}
        {% for entity in featureview.entities %}
            {% if entity not in all_entities %}
                {% set _ = all_entities.append(entity) %}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {% for entity in all_entities %}
    base."{{ entity }}",
    {% endfor %}
    {% set total_features = featureviews|map(attribute='features')|map('length')|sum %}
    {% set feature_counter = namespace(count=0) %}
    {% for featureview in featureviews %}
        {% set outer_loop_index = loop.index0 %}
        {% for feature in featureview.features %}
        {% set feature_counter.count = feature_counter.count + 1 %}
        fv_{{ outer_loop_index }}."{% if full_feature_names %}{{ featureview.name }}__{{ featureview.field_mapping.get(feature, feature) }}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}"{% if feature_counter.count < total_features %},{% endif %}
        {% endfor %}
    {% endfor %}
FROM base_entities base
{% for featureview in featureviews %}
{% set outer_loop_index = loop.index0 %}
LEFT JOIN LATERAL (
    SELECT DISTINCT ON ({% for entity in featureview.entities %}"{{ entity }}"{% if not loop.last %}, {% endif %}{% endfor %})
        event_timestamp,
        {% for entity in featureview.entities %}
        "{{ entity }}",
        {% endfor %}
        {% for feature in featureview.features %}
        "{% if full_feature_names %}{{ featureview.name }}__{{ featureview.field_mapping.get(feature, feature) }}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}"{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM "{{ featureview.name }}__data" fv_sub_{{ outer_loop_index }}
    WHERE fv_sub_{{ outer_loop_index }}.event_timestamp <= base.event_timestamp
    {% if featureview.ttl != 0 %}
    AND fv_sub_{{ outer_loop_index }}.event_timestamp >= base.event_timestamp - {{ featureview.ttl }} * interval '1' second
    {% endif %}
    {% for entity in featureview.entities %}
    AND fv_sub_{{ outer_loop_index }}."{{ entity }}" = base."{{ entity }}"
    {% endfor %}
    ORDER BY {% for entity in featureview.entities %}"{{ entity }}"{% if not loop.last %}, {% endif %}{% endfor %}, event_timestamp DESC
) AS fv_{{ outer_loop_index }} ON true
{% endfor %}
ORDER BY base.event_timestamp
{% endif %}
{% else %}
WITH
{% if use_cte %}
    entity_query AS ({{ left_table_query_string }}),
{% endif %}
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
entity_dataframe AS (
    SELECT *,
        {{entity_df_event_timestamp_col}} AS entity_timestamp
        {% for featureview in featureviews %}
            {% if featureview.entities %}
            ,(
                {% for entity in featureview.entities %}
                    CAST("{{entity}}" as VARCHAR) ||
                {% endfor %}
                CAST("{{entity_df_event_timestamp_col}}" AS VARCHAR)
            ) AS "{{featureview.name}}__entity_row_unique_id"
            {% else %}
            ,CAST("{{entity_df_event_timestamp_col}}" AS VARCHAR) AS "{{featureview.name}}__entity_row_unique_id"
            {% endif %}
        {% endfor %}
    FROM
        {% if use_cte %}
            entity_query
        {% else %}
            {{ left_table_query_string }}
        {% endif %}
)

{% if featureviews | length > 0 %}
,
{% endif %}

{% for featureview in featureviews %}

"{{ featureview.name }}__entity_dataframe" AS (
    SELECT
        {% if featureview.entities %}"{{ featureview.entities | join('", "') }}",{% endif %}
        entity_timestamp,
        "{{featureview.name}}__entity_row_unique_id"
    FROM entity_dataframe
    GROUP BY
        {% if featureview.entities %}"{{ featureview.entities | join('", "')}}",{% endif %}
        entity_timestamp,
        "{{featureview.name}}__entity_row_unique_id"
),

/*
 This query template performs the point-in-time correctness join for a single feature set table
 to the provided entity table.

 1. We first join the current feature_view to the entity dataframe that has been passed.
 This JOIN has the following logic:
    - For each row of the entity dataframe, only keep the rows where the `timestamp_field`
    is less than the one provided in the entity dataframe
    - If there a TTL for the current feature_view, also keep the rows where the `timestamp_field`
    is higher the the one provided minus the TTL
    - For each row, Join on the entity key and retrieve the `entity_row_unique_id` that has been
    computed previously

 The output of this CTE will contain all the necessary information and already filtered out most
 of the data that is not relevant.
*/

"{{ featureview.name }}__subquery" AS (
    SELECT
        "{{ featureview.timestamp_field }}" as event_timestamp,
        {{ '"' ~ featureview.created_timestamp_column ~ '" as created_timestamp,' if featureview.created_timestamp_column else '' }}
        {{ featureview.entity_selections | join(', ')}}{% if featureview.entity_selections %},{% else %}{% endif %}
        {% for feature in featureview.features %}
            "{{ feature }}" as {% if full_feature_names %}"{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}"{% else %}"{{ featureview.field_mapping.get(feature, feature) }}"{% endif %}{% if loop.last %}{% else %}, {% endif %}
        {% endfor %}
    FROM {{ featureview.table_subquery }} AS sub
    WHERE "{{ featureview.timestamp_field }}" <= (SELECT MAX(entity_timestamp) FROM entity_dataframe)
    {% if featureview.ttl == 0 %}{% else %}
    AND "{{ featureview.timestamp_field }}" >= (SELECT MIN(entity_timestamp) FROM entity_dataframe) - {{ featureview.ttl }} * interval '1' second
    {% endif %}
),

"{{ featureview.name }}__base" AS (
    SELECT
        subquery.*,
        entity_dataframe.entity_timestamp,
        entity_dataframe."{{featureview.name}}__entity_row_unique_id"
    FROM "{{ featureview.name }}__subquery" AS subquery
    INNER JOIN "{{ featureview.name }}__entity_dataframe" AS entity_dataframe
    ON TRUE
        AND subquery.event_timestamp <= entity_dataframe.entity_timestamp

        {% if featureview.ttl == 0 %}{% else %}
        AND subquery.event_timestamp >= entity_dataframe.entity_timestamp - {{ featureview.ttl }} * interval '1' second
        {% endif %}

        {% for entity in featureview.entities %}
        AND subquery."{{ entity }}" = entity_dataframe."{{ entity }}"
        {% endfor %}
),

/*
 2. If the `created_timestamp_column` has been set, we need to
 deduplicate the data first. This is done by calculating the
 `MAX(created_at_timestamp)` for each event_timestamp.
 We then join the data on the next CTE
*/
{% if featureview.created_timestamp_column %}
"{{ featureview.name }}__dedup" AS (
    SELECT
        "{{featureview.name}}__entity_row_unique_id",
        event_timestamp,
        MAX(created_timestamp) as created_timestamp
    FROM "{{ featureview.name }}__base"
    GROUP BY "{{featureview.name}}__entity_row_unique_id", event_timestamp
),
{% endif %}

/*
 3. The data has been filtered during the first CTE "*__base"
 Thus we only need to compute the latest timestamp of each feature.
*/
"{{ featureview.name }}__latest" AS (
    SELECT
        event_timestamp,
        {% if featureview.created_timestamp_column %}created_timestamp,{% endif %}
        "{{featureview.name}}__entity_row_unique_id"
    FROM
    (
        SELECT *,
            ROW_NUMBER() OVER(
                PARTITION BY "{{featureview.name}}__entity_row_unique_id"
                ORDER BY event_timestamp DESC{% if featureview.created_timestamp_column %},created_timestamp DESC{% endif %}
            ) AS row_number
        FROM "{{ featureview.name }}__base"
        {% if featureview.created_timestamp_column %}
            INNER JOIN "{{ featureview.name }}__dedup"
            USING ("{{featureview.name}}__entity_row_unique_id", event_timestamp, created_timestamp)
        {% endif %}
    ) AS sub
    WHERE row_number = 1
),

/*
 4. Once we know the latest value of each feature for a given timestamp,
 we can join again the data back to the original "base" dataset
*/
"{{ featureview.name }}__cleaned" AS (
    SELECT base.*
    FROM "{{ featureview.name }}__base" as base
    INNER JOIN "{{ featureview.name }}__latest"
    USING(
        "{{featureview.name}}__entity_row_unique_id",
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

SELECT "{{ final_output_feature_names | join('", "')}}"
FROM entity_dataframe
{% for featureview in featureviews %}
LEFT JOIN (
    SELECT
        "{{featureview.name}}__entity_row_unique_id"
        {% for feature in featureview.features %}
            ,"{% if full_feature_names %}{{ featureview.name }}__{{featureview.field_mapping.get(feature, feature)}}{% else %}{{ featureview.field_mapping.get(feature, feature) }}{% endif %}"
        {% endfor %}
    FROM "{{ featureview.name }}__cleaned"
) AS "{{featureview.name}}" USING ("{{featureview.name}}__entity_row_unique_id")
{% endfor %}
{% endif %}
"""
