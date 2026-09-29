from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import (
    Any,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

import pandas as pd
from trino.exceptions import TrinoQueryError

from feast import BatchFeatureView, FeatureView, StreamFeatureView
from feast.aggregation import Aggregation
from feast.data_source import DataSource
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.trino.sql_builder import TrinoQueryPlan
from feast.infra.compute_engines.trino.utils import (
    _trino_sql_literal,
    quote_identifier,
    stream_trino_arrow_batches,
    write_arrow_batches_to_online_store,
)
from feast.infra.compute_engines.utils import (
    ENTITY_TS_ALIAS,
    infer_entity_timestamp_column,
)
from feast.infra.offline_stores.contrib.trino_offline_store.connectors.upload import (
    upload_pandas_dataframe_to_trino,
)
from feast.infra.offline_stores.contrib.trino_offline_store.trino_queries import Trino
from feast.infra.offline_stores.offline_utils import get_temp_entity_table_name

logger = logging.getLogger(__name__)


def _format_agg_expression(agg: Aggregation) -> str:
    """Pure mapping of an Aggregation spec to its ANSI SQL Trino expression."""
    col_quoted = quote_identifier(agg.column)
    resolved_alias = quote_identifier(agg.resolved_name(agg.time_window))

    match agg.function.lower():
        case "count_distinct":
            expr = f"approx_distinct({col_quoted})"
        case "count" | "sum" | "avg" | "min" | "max" as func:
            expr = f"{func.upper()}({col_quoted})"
        case custom:
            expr = f"{custom.upper()}({col_quoted})"

    return f"{expr} AS {resolved_alias}"


class TrinoReadNode(DAGNode):
    """Generates the root Trino SQL CTE from the batch data source.

    Performs column projection, field_mapping aliasing, and timestamp filtering
    100% inside the Trino query without downloading raw source data.
    """

    def __init__(
        self,
        name: str,
        source: DataSource,
        column_info: ColumnInfo,
        client: Trino,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ):
        super().__init__(name)
        self.source = source
        self.column_info = column_info
        self.client = client
        self.start_time = start_time
        self.end_time = end_time

    def execute(self, context: ExecutionContext) -> DAGValue:
        table_ref = self.source.get_table_query_string()
        try:
            timestamp_col = self.column_info.timestamp_column
        except (ValueError, AttributeError):
            timestamp_col = None
        try:
            created_ts_col = self.column_info.created_timestamp_column
        except (ValueError, AttributeError):
            created_ts_col = None
        join_keys = self.column_info.join_keys
        field_mapping = self.column_info.field_mapping or {}

        cols_to_select = set(join_keys)
        if timestamp_col:
            cols_to_select.add(timestamp_col)
        if created_ts_col:
            cols_to_select.add(created_ts_col)
        cols_to_select.update(self.column_info.feature_cols)

        if cols_to_select:
            projections = [
                f"{quote_identifier(str(col))} AS {quote_identifier(field_mapping[str(col)])}"
                if str(col) in field_mapping and field_mapping[str(col)] != str(col)
                else quote_identifier(str(col))
                for col in sorted(cols_to_select, key=str)
            ]
            select_clause = ", ".join(projections)
        else:
            select_clause = "*"

        filters = []
        if timestamp_col and self.start_time:
            filters.append(
                f"{quote_identifier(timestamp_col)} >= {_trino_sql_literal(self.start_time)}"
            )
        if timestamp_col and self.end_time:
            filters.append(
                f"{quote_identifier(timestamp_col)} <= {_trino_sql_literal(self.end_time)}"
            )

        where_clause = f"\nWHERE {' AND '.join(filters)}" if filters else ""
        cte_name = f"_source_{self.name.replace(':', '_')}"
        query = f"SELECT {select_clause}\nFROM {table_ref}{where_clause}"

        plan = TrinoQueryPlan(
            ctes=((cte_name, query),),
            current_from=cte_name,
            columns=None,  # SELECT * from current CTE
            join_keys=tuple(join_keys),
            timestamp_col=timestamp_col,
            created_timestamp_col=created_ts_col,
            metadata={"source": self.name},
        )

        return DAGValue(data=plan, format=DAGFormat.TRINO)


class TrinoFilterNode(DAGNode):
    """Appends a Trino SQL CTE applying point-in-time, TTL, and custom filters."""

    def __init__(
        self,
        name: str,
        column_info: ColumnInfo,
        client: Trino,
        ttl: Optional[timedelta] = None,
        filter_condition: Optional[str] = None,
        inputs: Optional[Sequence[DAGNode]] = None,
    ):
        super().__init__(name, inputs=list(inputs) if inputs else None)
        self.column_info = column_info
        self.client = client
        self.ttl = ttl
        self.filter_condition = filter_condition

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.TRINO)
        upstream_plan: TrinoQueryPlan = input_value.data

        upstream_cte = upstream_plan.get_latest_cte_name()
        ts_col = upstream_plan.timestamp_col or self.column_info.timestamp_column
        ttl_seconds = int(self.ttl.total_seconds()) if self.ttl else None

        conditions = []
        if ts_col:
            conditions.append(
                f"{quote_identifier(ts_col)} <= {quote_identifier(ENTITY_TS_ALIAS)}"
            )
            if self.ttl:
                ttl_seconds = int(self.ttl.total_seconds())
                conditions.append(
                    f"{quote_identifier(ts_col)} >= {quote_identifier(ENTITY_TS_ALIAS)} - INTERVAL '{ttl_seconds}' SECOND"
                )
        if self.filter_condition:
            conditions.append(f"({self.filter_condition})")

        where_clause = f"\nWHERE {' AND '.join(conditions)}" if conditions else ""
        cte_name = f"_filter_{self.name.replace(':', '_')}"
        query = f"SELECT *\nFROM {upstream_cte}{where_clause}"

        plan = upstream_plan.add_cte(
            name=cte_name,
            query=query,
            metadata={"filter_applied": True},
        )
        return DAGValue(data=plan, format=DAGFormat.TRINO)


class TrinoDedupNode(DAGNode):
    """Appends a Trino SQL window CTE deduplicating rows to keep the latest per entity.

    Replaces Spark's Window.partitionBy().orderBy().row_number() with native Trino SQL:
    ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ... DESC) = 1.
    """

    def __init__(
        self,
        name: str,
        column_info: ColumnInfo,
        client: Trino,
        inputs: Optional[Sequence[DAGNode]] = None,
    ):
        super().__init__(name, inputs=list(inputs) if inputs else None)
        self.column_info = column_info
        self.client = client

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.TRINO)
        upstream_plan: TrinoQueryPlan = input_value.data

        upstream_cte = upstream_plan.get_latest_cte_name()
        join_keys = upstream_plan.join_keys or self.column_info.join_keys
        ts_col = upstream_plan.timestamp_col
        if not ts_col:
            try:
                ts_col = self.column_info.timestamp_column
            except (ValueError, AttributeError):
                ts_col = None

        created_ts_col = upstream_plan.created_timestamp_col
        if not created_ts_col:
            try:
                created_ts_col = self.column_info.created_timestamp_column
            except (ValueError, AttributeError):
                created_ts_col = None

        if not join_keys or not ts_col:
            return input_value

        partition_clause = ", ".join(quote_identifier(k) for k in join_keys)
        order_clauses = [f"{quote_identifier(ts_col)} DESC"]
        if created_ts_col:
            order_clauses.append(f"{quote_identifier(created_ts_col)} DESC")

        cte_name = f"_dedup_{self.name.replace(':', '_')}"
        query = (
            f"SELECT * FROM (\n"
            f"    SELECT *,\n"
            f"           ROW_NUMBER() OVER (\n"
            f"               PARTITION BY {partition_clause}\n"
            f"               ORDER BY {', '.join(order_clauses)}\n"
            f"           ) AS _feast_rn\n"
            f"    FROM {upstream_cte}\n"
            f") AS _dedup_sub\n"
            f"WHERE _feast_rn = 1"
        )

        plan = upstream_plan.add_cte(
            name=cte_name,
            query=query,
            metadata={"deduped": True},
        )
        return DAGValue(data=plan, format=DAGFormat.TRINO)


class TrinoAggregationNode(DAGNode):
    """Appends a Trino SQL aggregation CTE computing group by and windowed aggregates."""

    def __init__(
        self,
        name: str,
        aggregations: Sequence[Aggregation],
        group_by_keys: Sequence[str],
        timestamp_col: str,
        client: Trino,
        inputs: Optional[Sequence[DAGNode]] = None,
        enable_tiling: bool = False,
        hop_size: Optional[timedelta] = None,
    ):
        super().__init__(name, inputs=list(inputs) if inputs else None)
        self.aggregations: Tuple[Aggregation, ...] = tuple(aggregations)
        self.group_by_keys: Tuple[str, ...] = tuple(group_by_keys)
        self.timestamp_col: str = timestamp_col
        self.client: Trino = client
        self.enable_tiling: bool = enable_tiling
        self.hop_size: Optional[timedelta] = hop_size

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.TRINO)
        upstream_plan: TrinoQueryPlan = input_value.data

        upstream_cte = upstream_plan.get_latest_cte_name()

        # Pure list comprehension for aggregation expressions
        agg_exprs = [_format_agg_expression(agg) for agg in self.aggregations]
        group_by_clauses = [quote_identifier(k) for k in self.group_by_keys]

        has_time_windows = any(agg.time_window for agg in self.aggregations)

        if has_time_windows:
            time_window = self.aggregations[0].time_window
            assert time_window is not None
            window_sec = int(time_window.total_seconds())

            # Trino epoch bucketing expression
            window_bucket_expr = (
                f"from_unixtime(floor(to_unixtime({quote_identifier(self.timestamp_col)}) / "
                f"{window_sec}) * {window_sec})"
            )
            projections = (
                *group_by_clauses,
                f"{window_bucket_expr} AS {quote_identifier(self.timestamp_col)}",
                *agg_exprs,
            )
            group_by_all = (*group_by_clauses, window_bucket_expr)
        else:
            projections = (
                *group_by_clauses,
                f"MAX({quote_identifier(self.timestamp_col)}) AS {quote_identifier(self.timestamp_col)}",
                *agg_exprs,
            )
            group_by_all = tuple(group_by_clauses)

        cte_name = f"_agg_{self.name.replace(':', '_')}"
        query = (
            f"SELECT {', '.join(projections)}\n"
            f"FROM {upstream_cte}\n"
            f"GROUP BY {', '.join(group_by_all)}"
        )

        plan = upstream_plan.add_cte(
            name=cte_name,
            query=query,
            metadata={"aggregated": True},
        )
        return DAGValue(data=plan, format=DAGFormat.TRINO)


class TrinoJoinNode(DAGNode):
    """Joins upstream Trino SQL query plans or joins with entity_df."""

    def __init__(
        self,
        name: str,
        column_info: ColumnInfo,
        client: Trino,
        inputs: Optional[Sequence[DAGNode]] = None,
        how: str = "left",
    ):
        super().__init__(name, inputs=list(inputs) if inputs else [])
        self.column_info = column_info
        self.client = client
        self.how = how.upper()

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_values = self.get_input_values(context)
        for val in input_values:
            val.assert_format(DAGFormat.TRINO)

        upstream_plans: Tuple[TrinoQueryPlan, ...] = tuple(
            val.data for val in input_values
        )
        join_keys = self.column_info.join_keys

        # Pure tuple comprehension across input plans
        combined_ctes = tuple(cte for plan in upstream_plans for cte in plan.ctes)

        base_plan = upstream_plans[0]
        curr_from = base_plan.get_latest_cte_name()

        # Build join steps
        join_steps = []
        for plan in upstream_plans[1:]:
            next_cte = plan.get_latest_cte_name()
            if join_keys:
                on_conditions = " AND ".join(
                    f"{curr_from}.{quote_identifier(k)} = {next_cte}.{quote_identifier(k)}"
                    for k in join_keys
                )
                join_steps.append(f"{self.how} JOIN {next_cte} ON {on_conditions}")
            else:
                join_steps.append(f"CROSS JOIN {next_cte}")

        cte_name = f"_join_{self.name.replace(':', '_')}"
        query = f"SELECT * FROM {curr_from} {' '.join(join_steps)}"

        final_plan = TrinoQueryPlan(
            ctes=(*combined_ctes, (cte_name, query)),
            current_from=cte_name,
            join_keys=tuple(join_keys),
            timestamp_col=base_plan.timestamp_col,
            created_timestamp_col=base_plan.created_timestamp_col,
            metadata={"joined": True},
        )

        # Handle entity_df if provided in context
        if context.entity_df is not None:
            final_plan = self._join_with_entity_df(final_plan, context)

        return DAGValue(data=final_plan, format=DAGFormat.TRINO)

    def _join_with_entity_df(
        self, plan: TrinoQueryPlan, context: ExecutionContext
    ) -> TrinoQueryPlan:
        """Upload entity_df to staging table and join with point-in-time correctness."""
        entity_df = context.entity_df
        assert entity_df is not None

        if isinstance(entity_df, pd.DataFrame):
            catalog = getattr(context.repo_config.batch_engine, "catalog", "memory")
            dataset = getattr(context.repo_config.batch_engine, "dataset", "feast")
            tbl_name = get_temp_entity_table_name()
            full_tbl = f"{catalog}.{dataset}.{tbl_name}"

            # Upload entity dataframe to Trino
            connector = getattr(
                context.repo_config.offline_store,
                "connector",
                {"type": "memory"},
            )
            upload_pandas_dataframe_to_trino(
                client=self.client,
                df=entity_df,
                table=full_tbl,
                connector_args=connector,
            )

            entity_source_ref = full_tbl
            entity_ts_col = infer_entity_timestamp_column(
                dict(zip(entity_df.columns, entity_df.dtypes))
            )
        else:
            entity_source_ref = f"({entity_df})"
            entity_ts_col = ENTITY_TS_ALIAS

        latest_cte = plan.get_latest_cte_name()
        join_keys = plan.join_keys or self.column_info.join_keys

        on_cond = [
            f"{latest_cte}.{quote_identifier(k)} = _entity.{quote_identifier(k)}"
            for k in join_keys
        ]
        if plan.timestamp_col:
            on_cond.append(
                f"{latest_cte}.{quote_identifier(plan.timestamp_col)} <= _entity.{quote_identifier(entity_ts_col)}"
            )

        if on_cond:
            join_clause = f"LEFT JOIN {latest_cte} ON {' AND '.join(on_cond)}"
        else:
            join_clause = f"CROSS JOIN {latest_cte}"

        cte_name = f"_pit_join_{self.name.replace(':', '_')}"
        query = (
            f"SELECT _entity.*, {latest_cte}.*\n"
            f"FROM {entity_source_ref} AS _entity\n"
            f"{join_clause}"
        )

        return plan.add_cte(
            name=cte_name, query=query, metadata={"entity_joined": True}
        )


class TrinoTransformationNode(DAGNode):
    """Appends a Trino SQL CTE applying user-defined SQL feature transformations."""

    def __init__(
        self,
        name: str,
        udf: Any,
        inputs: Sequence[DAGNode],
        udf_string: str = "",
    ):
        super().__init__(name, list(inputs))
        self.udf = udf
        self.udf_string = udf_string

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_values = self.get_input_values(context)
        for val in input_values:
            val.assert_format(DAGFormat.TRINO)

        upstream_plans: Tuple[TrinoQueryPlan, ...] = tuple(
            val.data for val in input_values
        )
        primary_plan = upstream_plans[0]
        input_table_names = tuple(p.get_latest_cte_name() for p in upstream_plans)

        # Resolve SQL transformation string
        if callable(self.udf):
            sql_expr = self.udf(*input_table_names)
        elif isinstance(self.udf, str):
            sql_expr = (
                self.udf.format(*input_table_names)
                if "{}" in self.udf
                else f"SELECT * FROM {input_table_names[0]}"
            )
        elif self.udf_string:
            sql_expr = (
                self.udf_string.format(*input_table_names)
                if "{}" in self.udf_string
                else f"SELECT * FROM {input_table_names[0]}"
            )
        else:
            raise ValueError(
                f"TrinoTransformationNode '{self.name}' requires SQL UDF string or callable."
            )

        cte_name = f"_transform_{self.name.replace(':', '_')}"
        plan = primary_plan.add_cte(
            name=cte_name, query=sql_expr, metadata={"transformed": True}
        )
        return DAGValue(data=plan, format=DAGFormat.TRINO)


class TrinoValidationNode(DAGNode):
    """Validates schema projections and data invariants in Trino SQL."""

    def __init__(
        self,
        name: str,
        expected_columns: Mapping[str, Optional[str]],
        json_columns: Optional[Set[str]] = None,
        inputs: Optional[Sequence[DAGNode]] = None,
    ):
        super().__init__(name, inputs=list(inputs) if inputs else None)
        self.expected_columns: Mapping[str, Optional[str]] = dict(expected_columns)
        self.json_columns: frozenset[str] = frozenset(json_columns or ())

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.TRINO)
        context.node_outputs[self.name] = input_value
        return input_value


class TrinoWriteNode(DAGNode):
    """Terminal node executing Trino SQL plan and persisting to online or offline store.

    - Online Store: Streams PyArrow RecordBatches directly to online_write_batch.
    - Offline Store: Generates in-cluster CTAS / INSERT INTO via atomic staging + swap.
    """

    def __init__(
        self,
        name: str,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        client: Trino,
        inputs: Optional[Sequence[DAGNode]] = None,
    ):
        super().__init__(name, inputs=list(inputs) if inputs else None)
        self.feature_view = feature_view
        self.client = client

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.TRINO)
        plan: TrinoQueryPlan = input_value.data
        full_sql = plan.to_sql()

        # 1. Write to online store via streaming PyArrow batches
        if self.feature_view.online:
            logger.info("Executing online store materialization stream via Trino...")
            batches = stream_trino_arrow_batches(
                client=self.client,
                query_text=full_sql,
                batch_size=getattr(
                    context.repo_config.batch_engine, "batch_size", 10000
                ),
            )
            write_arrow_batches_to_online_store(
                batches=batches,
                feature_view=self.feature_view,
                online_store=context.online_store,
                repo_config=context.repo_config,
                concurrency=getattr(
                    context.repo_config.batch_engine, "write_concurrency", 4
                ),
            )

        # 2. Write to offline store using atomic staging + rename-swap pattern
        if getattr(self.feature_view, "offline", False):
            self._execute_offline_write(plan, context)

        return DAGValue(
            data=plan,
            format=DAGFormat.TRINO,
            metadata={
                "feature_view": self.feature_view.name,
                "online": self.feature_view.online,
                "offline": getattr(self.feature_view, "offline", False),
            },
        )

    def _execute_offline_write(
        self, plan: TrinoQueryPlan, context: ExecutionContext
    ) -> None:
        """Execute atomic materialization to an offline table via staging + rename-swap."""
        target_source = getattr(self.feature_view, "batch_source", None)
        if target_source is None:
            return

        target_table = target_source.get_table_query_string()
        staging_table = f"{target_table}__staging"

        try:
            self.client.execute_query(f"DROP TABLE IF EXISTS {staging_table}")
        except TrinoQueryError as e:
            logger.debug("Failed to drop staging table %s: %s", staging_table, e)

        # Step 1: Create staging table directly from compiled plan
        create_sql = f"CREATE TABLE {staging_table} AS (\n{plan.to_sql()}\n)"
        try:
            self.client.execute_query(create_sql)
            # Step 2: Atomic swap
            self.client.execute_query(f"DROP TABLE IF EXISTS {target_table}")
            tbl_simple_name = target_table.split(".")[-1]
            self.client.execute_query(
                f"ALTER TABLE {staging_table} RENAME TO {tbl_simple_name}"
            )
        except Exception:
            # Clean up staging table on failure; original target remains untouched
            try:
                self.client.execute_query(f"DROP TABLE IF EXISTS {staging_table}")
            except TrinoQueryError as cleanup_err:
                logger.debug(
                    "Staging table cleanup error on %s: %s",
                    staging_table,
                    cleanup_err,
                )
            raise
