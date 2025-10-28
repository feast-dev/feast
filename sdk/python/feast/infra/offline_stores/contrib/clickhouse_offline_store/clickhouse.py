import contextlib
import re
from dataclasses import asdict
from datetime import datetime
from typing import Iterator, List, Literal, Optional, Union, cast

import numpy as np
import pandas as pd
import pyarrow as pa
from pyarrow.compute import cast as pa_cast

from feast import FeatureView, OnDemandFeatureView, RepoConfig
from feast.data_source import DataSource
from feast.errors import EntitySQLEmptyResults, InvalidEntityType
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL
from feast.infra.offline_stores import offline_utils
from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source import (
    ClickhouseSource,
    SavedDatasetClickhouseStorage,
)
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
    PostgreSQLRetrievalJob,
    build_point_in_time_query,
)
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.registry.base_registry import BaseRegistry
from feast.infra.utils.clickhouse.clickhouse_config import ClickhouseConfig
from feast.infra.utils.clickhouse.connection_utils import get_client
from feast.saved_dataset import SavedDatasetStorage


class ClickhouseOfflineStoreConfig(ClickhouseConfig):
    type: Literal["clickhouse"] = "clickhouse"


class ClickhouseOfflineStore(OfflineStore):
    @staticmethod
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: BaseRegistry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        assert isinstance(config.offline_store, ClickhouseOfflineStoreConfig)
        for fv in feature_views:
            assert isinstance(fv.batch_source, ClickhouseSource)

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
            if (
                isinstance(entity_df, pd.DataFrame)
                and not config.offline_store.use_temporary_tables_for_entity_df
            ):
                table_name = f"{config.offline_store.database}.{table_name}"

            _upload_entity_df(
                config,
                entity_df,
                table_name,
                entity_df_event_timestamp_col,
            )

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
                query = build_point_in_time_query(
                    query_context_dict,
                    left_table_query_string=table_name,
                    entity_df_event_timestamp_col=entity_df_event_timestamp_col,
                    entity_df_columns=entity_schema.keys(),
                    query_template=MULTIPLE_FEATURE_VIEW_POINT_IN_TIME_JOIN,
                    full_feature_names=full_feature_names,
                )
                yield query
            finally:
                if (
                    table_name
                    and not config.offline_store.use_temporary_tables_for_entity_df
                ):
                    get_client(config.offline_store).command(
                        f"DROP TABLE IF EXISTS {table_name}"
                    )

        return ClickhouseRetrievalJob(
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
        assert isinstance(config.offline_store, ClickhouseOfflineStoreConfig)
        assert isinstance(data_source, ClickhouseSource)
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
                WHERE a."{timestamp_field}"
                    BETWEEN toDateTime64('{start_date.replace(tzinfo=None)!s}', 6, '{start_date.tzinfo!s}')
                    AND toDateTime64('{end_date.replace(tzinfo=None)!s}', 6, '{end_date.tzinfo!s}')
            ) b
            WHERE _feast_row = 1
            """

        return ClickhouseRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
            on_demand_feature_views=None,
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
        assert isinstance(config.offline_store, ClickhouseOfflineStoreConfig)
        assert isinstance(data_source, ClickhouseSource)

        from_expression = data_source.get_table_query_string()

        timestamp_fields = [timestamp_field]

        if created_timestamp_column:
            timestamp_fields.append(created_timestamp_column)

        field_string = ", ".join(
            join_key_columns + feature_name_columns + timestamp_fields
        )

        query = f"""
            SELECT {field_string}
            FROM {from_expression}
            WHERE {timestamp_field} BETWEEN parseDateTimeBestEffort('{start_date}') AND parseDateTimeBestEffort('{end_date}')
        """

        return ClickhouseRetrievalJob(
            query=query,
            config=config,
            full_feature_names=False,
        )


class ClickhouseRetrievalJob(PostgreSQLRetrievalJob):
    def _to_df_internal(self, timeout: Optional[int] = None) -> pd.DataFrame:
        with self._query_generator() as query:
            results = get_client(self.config.offline_store).query_df(query)
            return results

    def _to_arrow_internal(self, timeout: Optional[int] = None) -> pa.Table:
        with self._query_generator() as query:
            results: pa.Table = get_client(self.config.offline_store).query_arrow(query)
            # Feast doesn't support native decimal types, so we must convert decimal columns to double
            for col_index, (name, dtype) in enumerate(
                zip(results.schema.names, results.schema.types)
            ):
                if pa.types.is_decimal(dtype):
                    results = results.set_column(
                        col_index,
                        name,
                        pa_cast(results[name], target_type=pa.float64()),
                    )
            return results

    def persist(
        self,
        storage: SavedDatasetStorage,
        allow_overwrite: Optional[bool] = False,
        timeout: Optional[int] = None,
    ):
        assert isinstance(storage, SavedDatasetClickhouseStorage)

        df_to_clickhouse_table(
            config=self.config.offline_store,
            df=self.to_df(),
            table_name=storage.clickhouse_options._table,
            entity_timestamp_col="event_timestamp",
        )


def _get_entity_schema(
    entity_df: Union[pd.DataFrame, str],
    config: RepoConfig,
) -> dict[str, np.dtype]:
    if isinstance(entity_df, pd.DataFrame):
        return dict(zip(entity_df.columns, entity_df.dtypes))
    elif isinstance(entity_df, str):
        query = f"SELECT * FROM ({entity_df}) LIMIT 1"
        df = get_client(config.offline_store).query_df(query)
        return _get_entity_schema(df, config)
    else:
        raise InvalidEntityType(type(entity_df))


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str],
    entity_df_event_timestamp_col: str,
    config: RepoConfig,
) -> tuple[datetime, datetime]:
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
        query = f'SELECT MIN("{entity_df_event_timestamp_col}") AS "min_value", MAX("{entity_df_event_timestamp_col}") AS "max_value" FROM ({entity_df})'
        results = get_client(config.offline_store).query(query).result_rows

        entity_df_event_timestamp_range = cast(tuple[datetime, datetime], results[0])
        if (
            entity_df_event_timestamp_range[0] is None
            or entity_df_event_timestamp_range[1] is None
        ):
            raise EntitySQLEmptyResults(entity_df)
    else:
        raise InvalidEntityType(type(entity_df))

    return entity_df_event_timestamp_range


def _upload_entity_df(
    config: RepoConfig,
    entity_df: Union[pd.DataFrame, str],
    table_name: str,
    entity_timestamp_col: str,
) -> None:
    if isinstance(entity_df, pd.DataFrame):
        df_to_clickhouse_table(
            config.offline_store, entity_df, table_name, entity_timestamp_col
        )
    elif isinstance(entity_df, str):
        if config.offline_store.use_temporary_tables_for_entity_df:
            query = f'CREATE TEMPORARY TABLE "{table_name}" AS ({entity_df})'
        else:
            query = f'CREATE TABLE "{table_name}" ENGINE = MergeTree() ORDER BY ({entity_timestamp_col}) AS ({entity_df})'
        get_client(config.offline_store).command(query)
    else:
        raise InvalidEntityType(type(entity_df))


def df_to_clickhouse_table(
    config: ClickhouseConfig,
    df: pd.DataFrame,
    table_name: str,
    entity_timestamp_col: str,
) -> None:
    table_schema = _df_to_create_table_schema(df)
    if config.use_temporary_tables_for_entity_df:
        query = f"CREATE TEMPORARY TABLE {table_name} ({table_schema})"
    else:
        query = f"""
            CREATE TABLE {table_name} (
                {table_schema}
            )
            ENGINE = MergeTree()
            ORDER BY ({entity_timestamp_col})
        """
    get_client(config).command(query)
    get_client(config).insert_df(table_name, df)


def _df_to_create_table_schema(entity_df: pd.DataFrame) -> str:
    pa_table = pa.Table.from_pandas(entity_df)
    columns = [
        f""""{f.name}" {arrow_to_ch_type(str(f.type), f.nullable)}"""
        for f in pa_table.schema
    ]
    return ", ".join(columns)


def arrow_to_ch_type(t_str: str, nullable: bool) -> str:
    list_pattern = r"list<item: (.*)>"
    list_res = re.search(list_pattern, t_str)
    if list_res is not None:
        item_type_str = list_res.group(1)
        return f"Array({arrow_to_ch_type(item_type_str, nullable)})"

    if nullable:
        return f"Nullable({arrow_to_ch_type(t_str, nullable=False)})"

    try:
        if t_str.startswith("timestamp"):
            return _arrow_to_ch_timestamp_type(t_str)
        return {
            "bool": "Boolean",
            "int8": "Int8",
            "int16": "Int16",
            "int32": "Int32",
            "int64": "Int64",
            "uint8": "UInt8",
            "uint16": "UInt16",
            "uint32": "UInt32",
            "uint64": "Uint64",
            "float": "Float32",
            "double": "Float64",
            "string": "String",
        }[t_str]
    except KeyError:
        raise ValueError(f"Unsupported type: {t_str}")


def _arrow_to_ch_timestamp_type(t_str: str) -> str:
    _ARROW_PRECISION_TO_CH_PRECISION = {
        "s": 0,
        "ms": 3,
        "us": 6,
        "ns": 9,
    }

    unit, *rest = t_str.removeprefix("timestamp[").removesuffix("]").split(",")

    unit = unit.strip()
    precision = _ARROW_PRECISION_TO_CH_PRECISION[unit]

    if len(rest):
        tz = rest[0]
        tz = (
            tz.strip()
            .removeprefix("tz=")
            .translate(
                str.maketrans(
                    {  # type: ignore[arg-type]
                        "'": None,
                        '"': None,
                    }
                )
            )
        )
    else:
        tz = None

    if precision > 0:
        if tz is not None:
            return f"DateTime64({precision}, '{tz}')"
        else:
            return f"DateTime64({precision})"
    else:
        if tz is not None:
            return f"DateTime('{tz}')"
        else:
            return "DateTime"


def _append_alias(field_names: List[str], alias: str) -> List[str]:
    return [f'{alias}."{field_name}"' for field_name in field_names]


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
    AND "{{ featureview.timestamp_field }}" >= (SELECT MIN(entity_timestamp) FROM entity_dataframe) - interval {{ featureview.ttl }} second
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
        {% for entity in featureview.entities %}
        AND subquery."{{ entity }}" = entity_dataframe."{{ entity }}"
        {% endfor %}
    WHERE TRUE
        AND subquery.event_timestamp <= entity_dataframe.entity_timestamp

        {% if featureview.ttl == 0 %}{% else %}
        AND subquery.event_timestamp >= entity_dataframe.entity_timestamp - interval {{ featureview.ttl }} second
        {% endif %}
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
