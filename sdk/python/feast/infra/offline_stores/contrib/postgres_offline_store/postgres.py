import contextlib
from dataclasses import asdict
from datetime import datetime
from typing import (
    Any,
    Callable,
    ContextManager,
    Iterator,
    KeysView,
    List,
    Optional,
    Tuple,
    Union,
)

import pandas as pd
import pyarrow as pa
from jinja2 import BaseLoader, Environment
from psycopg2 import sql
from pydantic.typing import Literal
from pytz import utc

from feast.data_source import DataSource
from feast.errors import InvalidEntityType
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.utils.postgres.connection_utils import (
    _get_conn,
    df_to_postgres_table,
    get_query_schema,
)
from feast.infra.utils.postgres.postgres_config import PostgreSQLConfig
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.registry import Registry
from feast.repo_config import RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.type_map import pg_type_code_to_arrow
from feast.usage import log_exceptions_and_usage

from .postgres_source import PostgreSQLSource


class PostgreSQLOfflineStoreConfig(PostgreSQLConfig):
    type: Literal["postgres"] = "postgres"


class PostgreSQLOfflineStore(OfflineStore):
    @staticmethod
    @log_exceptions_and_usage(offline_store="postgres")
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
                FROM ({from_expression}) a
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
    @log_exceptions_and_usage(offline_store="postgres")
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        @contextlib.contextmanager
        def query_generator() -> Iterator[str]:
            table_name = None
            if isinstance(entity_df, pd.DataFrame):
                table_name = offline_utils.get_temp_entity_table_name()
                entity_schema = df_to_postgres_table(
                    config.offline_store, entity_df, table_name
                )
                df_query = table_name
            elif isinstance(entity_df, str):
                df_query = f"({entity_df}) AS sub"
                entity_schema = get_query_schema(config.offline_store, df_query)
            else:
                raise TypeError(entity_df)

            entity_df_event_timestamp_col = (
                offline_utils.infer_event_timestamp_from_entity_df(entity_schema)
            )

            expected_join_keys = offline_utils.get_expected_join_keys(
                project, feature_views, registry
            )

            offline_utils.assert_expected_columns_in_entity_df(
                entity_schema, expected_join_keys, entity_df_event_timestamp_col
            )

            entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
                entity_df,
                entity_df_event_timestamp_col,
                config,
                df_query,
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
                    f'''"{entity_selection.replace(' AS ', '" AS "')}\"'''
                    for entity_selection in context["entity_selections"]
                ]

            try:
                yield build_point_in_time_query(
                    query_context_dict,
                    left_table_query_string=df_query,
                    entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                    entity_df_columns=entity_schema.keys(),
                    query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                    full_feature_names=full_feature_names,
                )
            finally:
                if table_name:
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
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="postgres")
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        timestamp_field: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(data_source, PostgreSQLSource)
        from_expression = data_source.get_table_query_string()

        field_string = ", ".join(
            join_key_columns + feature_name_columns + [timestamp_field]
        )

        start_date = start_date.astimezone(tz=utc)
        end_date = end_date.astimezone(tz=utc)

        query = f"""
            SELECT {field_string}
            FROM {from_expression} AS paftoq_alias
            WHERE "{timestamp_field}" BETWEEN '{start_date}'::timestamptz AND '{end_date}'::timestamptz
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
        on_demand_feature_views: Optional[List[OnDemandFeatureView]],
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
        self._on_demand_feature_views = on_demand_feature_views
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> Optional[List[OnDemandFeatureView]]:
        return self._on_demand_feature_views

    def _to_df_internal(self) -> pd.DataFrame:
        # We use arrow format because it gives better control of the table schema
        return self._to_arrow_internal().to_pandas()

    def to_sql(self) -> str:
        with self._query_generator() as query:
            return query

    def _to_arrow_internal(self) -> pa.Table:
        with self._query_generator() as query:
            with _get_conn(self.config.offline_store) as conn, conn.cursor() as cur:
                conn.set_session(readonly=True)
                cur.execute(query)
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

    def persist(self, storage: SavedDatasetStorage):
        pass


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    config: RepoConfig,
    table_name: str,
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
            entity_df_event_timestamp.min(),
            entity_df_event_timestamp.max(),
        )
    elif isinstance(entity_df, str):
        # If the entity_df is a string (SQL query), determine range
        # from table
        with _get_conn(config.offline_store) as conn, conn.cursor() as cur:
            cur.execute(
                f"SELECT MIN({entity_df_event_timestamp_col}) AS min, MAX({entity_df_event_timestamp_col}) AS max FROM {table_name}"
            ),
            res = cur.fetchone()
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
) -> str:
    """Build point-in-time query between each feature view table and the entity dataframe for PostgreSQL"""
    template = Environment(loader=BaseLoader()).from_string(source=query_template)

    final_output_feature_names = list(entity_df_columns)
    final_output_feature_names.extend(
        [
            (
                f'{fv["name"]}__{fv["field_mapping"].get(feature, feature)}'
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
    }

    query = template.render(template_context)
    return query


# Copied from the Feast Redshift offline store implementation
# Note: Keep this in sync with sdk/python/feast/infra/offline_stores/redshift.py:
# MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN
# https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/offline_stores/redshift.py

MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN = """
/*
 Compute a deterministic hash for the `left_table_query_string` that will be used throughout
 all the logic as the field to GROUP BY the data
*/
WITH entity_dataframe AS (
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
    FROM {{ left_table_query_string }}
),

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
"""
