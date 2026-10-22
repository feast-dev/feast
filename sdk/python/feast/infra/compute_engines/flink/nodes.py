from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Union

import pandas as pd
import pyarrow as pa

from feast import BatchFeatureView, FeatureView, StreamFeatureView
from feast.aggregation import Aggregation, aggregation_specs_to_agg_ops
from feast.data_source import DataSource
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.flink.utils import (
    flink_table_to_arrow_batches,
    pandas_to_flink_table,
    register_flink_temporary_view,
)
from feast.infra.compute_engines.utils import (
    ENTITY_ROW_ID,
    ENTITY_TS_ALIAS,
    create_offline_store_retrieval_job,
    find_entity_timestamp_column,
    infer_entity_timestamp_column,
)
from feast.utils import _convert_arrow_to_proto

logger = logging.getLogger(__name__)

DEDUP_ROW_NUMBER = "__feast_row_number"


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _qualified_column(alias: str, column: str) -> str:
    return f"{alias}.{_quote_identifier(column)}"


def _select_column(alias: str, column: str, output_name: Optional[str] = None) -> str:
    expr = _qualified_column(alias, column)
    if output_name and output_name != column:
        return f"{expr} AS {_quote_identifier(output_name)}"
    return expr


def _flink_interval_literals(value: timedelta) -> List[str]:
    total_seconds = int(value.total_seconds())
    if total_seconds <= 0:
        return ["INTERVAL '0' SECOND"]

    days, remainder = divmod(total_seconds, 24 * 60 * 60)
    hours, remainder = divmod(remainder, 60 * 60)
    minutes, seconds = divmod(remainder, 60)
    parts: List[str] = []
    if days:
        parts.append(f"INTERVAL '{days}' DAY")
    if hours:
        parts.append(f"INTERVAL '{hours}' HOUR")
    if minutes:
        parts.append(f"INTERVAL '{minutes}' MINUTE")
    if seconds:
        parts.append(f"INTERVAL '{seconds}' SECOND")
    return parts


def _subtract_flink_intervals(timestamp_expr: str, value: timedelta) -> str:
    result = timestamp_expr
    for interval in _flink_interval_literals(value):
        result = f"{result} - {interval}"
    return result


def _get_columns_from_schema(table: Any) -> Optional[List[str]]:
    if not hasattr(table, "get_schema"):
        return None
    schema = table.get_schema()
    if hasattr(schema, "get_field_names"):
        return list(schema.get_field_names())
    if hasattr(schema, "get_field_count") and hasattr(schema, "get_field_name"):
        return [schema.get_field_name(i) for i in range(schema.get_field_count())]
    return None


def _get_columns(value: DAGValue) -> List[str]:
    metadata_columns = value.metadata.get("columns") if value.metadata else None
    if metadata_columns:
        return list(metadata_columns)
    schema_columns = _get_columns_from_schema(value.data)
    if schema_columns:
        return schema_columns
    raise ValueError(
        "Could not infer columns for Flink DAG value from metadata or PyFlink schema."
    )


def _can_use_sql(table_env: Any) -> bool:
    return hasattr(table_env, "create_temporary_view") and hasattr(
        table_env, "sql_query"
    )


def _require_sql(table_env: Any, node_name: str) -> None:
    if not _can_use_sql(table_env):
        raise RuntimeError(
            f"Flink node '{node_name}' requires a PyFlink TableEnvironment with "
            "create_temporary_view() and sql_query()."
        )


def _register_table(table_env: Any, table: Any, prefix: str) -> str:
    view_name = f"__feast_{prefix}_{uuid.uuid4().hex}"
    table_env.create_temporary_view(view_name, table)
    register_flink_temporary_view(table_env, view_name)
    return view_name


def _sql_value(
    table_env: Any,
    query: str,
    columns: Iterable[str],
    metadata: Optional[dict] = None,
) -> DAGValue:
    return DAGValue(
        data=table_env.sql_query(query),
        format=DAGFormat.FLINK,
        metadata={**(metadata or {}), "columns": list(columns), "native_sql": query},
    )


def _entity_timestamp_column_from_columns(columns: List[str]) -> str:
    entity_ts_col = find_entity_timestamp_column(columns)
    if entity_ts_col:
        return entity_ts_col
    raise ValueError(
        "SQL-based entity_df for FlinkComputeEngine must select an "
        "`event_timestamp` column."
    )


def _entity_value_from_dataframe(
    table_env: Any,
    entity_df: pd.DataFrame,
    split_num: int,
    join_keys: List[str],
) -> tuple[Any, List[str], str]:
    entity_df = entity_df.copy()
    if entity_df.empty:
        for join_key in join_keys:
            if join_key not in entity_df.columns:
                entity_df[join_key] = pd.Series(dtype="object")
        entity_ts_col = find_entity_timestamp_column(list(entity_df.columns))
        if entity_ts_col is None:
            entity_ts_col = ENTITY_TS_ALIAS
            entity_df[entity_ts_col] = pd.Series(dtype="datetime64[ns]")
    else:
        entity_schema = dict(zip(entity_df.columns, entity_df.dtypes))
        entity_ts_col = infer_entity_timestamp_column(entity_schema)

    entity_df[ENTITY_ROW_ID] = range(len(entity_df))
    if entity_ts_col != ENTITY_TS_ALIAS:
        entity_df = entity_df.rename(columns={entity_ts_col: ENTITY_TS_ALIAS})
    return (
        pandas_to_flink_table(table_env, entity_df, split_num),
        list(entity_df.columns),
        entity_ts_col,
    )


def _entity_value_from_sql(
    table_env: Any,
    entity_sql: str,
    join_keys: List[str],
) -> tuple[Any, List[str], str]:
    _require_sql(table_env, "entity_df")
    entity_table = table_env.sql_query(entity_sql)
    entity_columns = _get_columns_from_schema(entity_table)
    if entity_columns is None:
        raise ValueError("Could not infer columns for SQL-based entity_df.")

    entity_ts_col = _entity_timestamp_column_from_columns(entity_columns)
    entity_view = _register_table(table_env, entity_table, "entity_sql")
    output_columns = [
        ENTITY_TS_ALIAS if column == entity_ts_col else column
        for column in entity_columns
    ]
    select_exprs = [
        _select_column(
            "entity_src",
            column,
            ENTITY_TS_ALIAS if column == entity_ts_col else column,
        )
        for column in entity_columns
    ]
    order_columns = [
        column for column in [entity_ts_col, *join_keys] if column in entity_columns
    ]
    order_expr = ", ".join(
        _qualified_column("entity_src", col) for col in order_columns
    )
    if not order_expr:
        order_expr = _qualified_column("entity_src", entity_columns[0])
    select_exprs.append(
        f"ROW_NUMBER() OVER (ORDER BY {order_expr}) - 1 AS "
        f"{_quote_identifier(ENTITY_ROW_ID)}"
    )
    query = (
        f"SELECT {', '.join(select_exprs)} "
        f"FROM {_quote_identifier(entity_view)} AS entity_src"
    )
    output_columns.append(ENTITY_ROW_ID)
    value = _sql_value(
        table_env,
        query,
        output_columns,
        metadata={"entity_timestamp_column": entity_ts_col},
    )
    return value.data, output_columns, entity_ts_col


def _entity_value_from_context(
    table_env: Any,
    context: ExecutionContext,
    split_num: int,
    join_keys: List[str],
) -> tuple[Any, List[str], str]:
    if isinstance(context.entity_df, pd.DataFrame):
        return _entity_value_from_dataframe(
            table_env, context.entity_df, split_num, join_keys
        )
    if isinstance(context.entity_df, str):
        return _entity_value_from_sql(table_env, context.entity_df, join_keys)
    raise TypeError(
        "FlinkComputeEngine entity_df must be a pandas DataFrame, SQL string, or None."
    )


def _retrieval_job_to_flink_table(
    retrieval_job: Any,
    table_env: Any,
    split_num: int,
) -> tuple[Any, List[str]]:
    to_flink_table = getattr(retrieval_job, "to_flink_table", None)
    if callable(to_flink_table):
        flink_table = to_flink_table(table_env)
        columns = _get_columns_from_schema(flink_table)
        if columns is None:
            raise ValueError(
                "Could not infer columns for source Flink table returned by "
                "RetrievalJob.to_flink_table(table_env)."
            )
        return flink_table, columns

    if not hasattr(retrieval_job, "to_arrow"):
        raise TypeError(
            "FlinkComputeEngine source reads require a RetrievalJob with either "
            "to_flink_table(table_env) or to_arrow()."
        )

    arrow_table = retrieval_job.to_arrow()
    if not isinstance(arrow_table, pa.Table):
        raise TypeError(
            "RetrievalJob.to_arrow() must return a pyarrow.Table for "
            "FlinkComputeEngine source reads."
        )
    columns = list(arrow_table.column_names)
    return pandas_to_flink_table(table_env, arrow_table.to_pandas(), split_num), columns


class FlinkSourceReadNode(DAGNode):
    def __init__(
        self,
        name: str,
        source: DataSource,
        column_info: ColumnInfo,
        table_env: Any,
        split_num: int,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ) -> None:
        super().__init__(name)
        self.source = source
        self.column_info = column_info
        self.table_env = table_env
        self.split_num = split_num
        self.start_time = start_time
        self.end_time = end_time

    def execute(self, context: ExecutionContext) -> DAGValue:
        retrieval_job = create_offline_store_retrieval_job(
            data_source=self.source,
            column_info=self.column_info,
            context=context,
            start_time=self.start_time,
            end_time=self.end_time,
        )
        flink_table, columns = _retrieval_job_to_flink_table(
            retrieval_job, self.table_env, self.split_num
        )

        if self.column_info.field_mapping:
            view_name = _register_table(self.table_env, flink_table, "source_read")
            select_exprs = [
                _select_column(
                    "src",
                    col,
                    self.column_info.field_mapping.get(col, col),
                )
                for col in columns
            ]
            renamed_columns = [
                self.column_info.field_mapping.get(col, col) for col in columns
            ]
            query = (
                f"SELECT {', '.join(select_exprs)} "
                f"FROM {_quote_identifier(view_name)} AS src"
            )
            return _sql_value(
                self.table_env,
                query,
                renamed_columns,
                metadata={
                    "source": "feature_view_batch_source",
                    "timestamp_field": self.column_info.timestamp_column,
                    "created_timestamp_column": (
                        self.column_info.created_timestamp_column
                    ),
                    "start_date": self.start_time,
                    "end_date": self.end_time,
                },
            )

        return DAGValue(
            data=flink_table,
            format=DAGFormat.FLINK,
            metadata={
                "source": "feature_view_batch_source",
                "timestamp_field": self.column_info.timestamp_column,
                "created_timestamp_column": (self.column_info.created_timestamp_column),
                "start_date": self.start_time,
                "end_date": self.end_time,
                "columns": columns,
            },
        )


class FlinkJoinNode(DAGNode):
    def __init__(
        self,
        name: str,
        column_info: ColumnInfo,
        table_env: Any,
        split_num: int,
        inputs: Optional[List[DAGNode]] = None,
        how: str = "left",
    ) -> None:
        super().__init__(name, inputs=inputs or [])
        self.column_info = column_info
        self.table_env = table_env
        self.split_num = split_num
        self.how = how

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_values = self.get_input_values(context)
        for value in input_values:
            value.assert_format(DAGFormat.FLINK)
        if not input_values:
            raise RuntimeError(f"FlinkJoinNode '{self.name}' requires inputs")

        _require_sql(self.table_env, self.name)
        return self._execute_sql_join(input_values, context)

    def _execute_sql_join(
        self, input_values: List[DAGValue], context: ExecutionContext
    ) -> DAGValue:
        join_keys = self.column_info.join_keys_columns
        view_names = [
            _register_table(self.table_env, value.data, f"join_{index}")
            for index, value in enumerate(input_values)
        ]
        columns_by_input = [_get_columns(value) for value in input_values]
        output_columns = list(columns_by_input[0])
        seen_columns = set(output_columns)
        select_exprs = [_select_column("t0", column) for column in columns_by_input[0]]

        joins = []
        for index, view_name in enumerate(view_names[1:], start=1):
            alias = f"t{index}"
            on_clause = " AND ".join(
                f"{_qualified_column('t0', key)} = {_qualified_column(alias, key)}"
                for key in join_keys
            )
            joins.append(
                f"{self.how.upper()} JOIN {_quote_identifier(view_name)} AS {alias} "
                f"ON {on_clause}"
            )
            for column in columns_by_input[index]:
                if column in join_keys or column in seen_columns:
                    continue
                output_columns.append(column)
                seen_columns.add(column)
                select_exprs.append(_select_column(alias, column))

        query = (
            f"SELECT {', '.join(select_exprs)} "
            f"FROM {_quote_identifier(view_names[0])} AS t0 "
            f"{' '.join(joins)}"
        )
        joined_value = _sql_value(
            self.table_env,
            query,
            output_columns,
            metadata={"joined_on": join_keys, "join_type": self.how},
        )

        if context.entity_df is None:
            return joined_value

        entity_table, entity_columns, entity_ts_col = _entity_value_from_context(
            self.table_env, context, self.split_num, join_keys
        )
        entity_view = _register_table(self.table_env, entity_table, "entity")
        feature_view = _register_table(self.table_env, joined_value.data, "features")
        feature_columns = [
            column
            for column in output_columns
            if column not in join_keys and column not in entity_columns
        ]
        select_entity = [_select_column("e", column) for column in entity_columns]
        select_features = [_select_column("f", column) for column in feature_columns]
        on_clause = " AND ".join(
            f"{_qualified_column('e', key)} = {_qualified_column('f', key)}"
            for key in join_keys
        )
        entity_join_query = (
            f"SELECT {', '.join(select_entity + select_features)} "
            f"FROM {_quote_identifier(entity_view)} AS e "
            f"LEFT JOIN {_quote_identifier(feature_view)} AS f ON {on_clause}"
        )
        return _sql_value(
            self.table_env,
            entity_join_query,
            entity_columns + feature_columns,
            metadata={
                "joined_on": join_keys,
                "join_type": "left",
                "entity_timestamp_column": entity_ts_col,
            },
        )


class FlinkFilterNode(DAGNode):
    def __init__(
        self,
        name: str,
        column_info: ColumnInfo,
        table_env: Any,
        split_num: int,
        filter_expr: Optional[str] = None,
        ttl: Optional[timedelta] = None,
        inputs: Optional[List[DAGNode]] = None,
    ) -> None:
        super().__init__(name, inputs=inputs)
        self.column_info = column_info
        self.table_env = table_env
        self.split_num = split_num
        self.filter_expr = filter_expr
        self.ttl = ttl

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.FLINK)

        _require_sql(self.table_env, self.name)
        return self._execute_sql_filter(input_value)

    def _execute_sql_filter(self, input_value: DAGValue) -> DAGValue:
        columns = _get_columns(input_value)
        timestamp_column = self.column_info.timestamp_column
        conditions = []

        if ENTITY_TS_ALIAS in columns and timestamp_column in columns:
            conditions.append(
                f"{_quote_identifier(timestamp_column)} <= "
                f"{_quote_identifier(ENTITY_TS_ALIAS)}"
            )
            if self.ttl:
                lower_bound = _subtract_flink_intervals(
                    _quote_identifier(ENTITY_TS_ALIAS), self.ttl
                )
                conditions.append(
                    f"{_quote_identifier(timestamp_column)} >= {lower_bound}"
                )

        if self.filter_expr:
            conditions.append(f"({self.filter_expr})")

        if not conditions:
            return input_value

        view_name = _register_table(self.table_env, input_value.data, "filter")
        query = (
            f"SELECT * FROM {_quote_identifier(view_name)} "
            f"WHERE {' AND '.join(conditions)}"
        )
        return _sql_value(
            self.table_env,
            query,
            columns,
            metadata={**(input_value.metadata or {}), "filter_applied": True},
        )


class FlinkAggregationNode(DAGNode):
    def __init__(
        self,
        name: str,
        group_keys: List[str],
        aggregations: List[Aggregation],
        table_env: Any,
        split_num: int,
        inputs: Optional[List[DAGNode]] = None,
    ) -> None:
        super().__init__(name, inputs=inputs)
        self.group_keys = group_keys
        self.aggregations = aggregations
        self.table_env = table_env
        self.split_num = split_num

    def execute(self, context: ExecutionContext) -> DAGValue:
        agg_ops = aggregation_specs_to_agg_ops(
            self.aggregations,
            time_window_unsupported_error_message=(
                "Time window aggregation is not yet supported in the Flink compute "
                "engine. Use non-windowed aggregations or pre-window upstream in Flink."
            ),
        )
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.FLINK)

        _require_sql(self.table_env, self.name)
        return self._execute_sql_aggregation(input_value, agg_ops)

    def _execute_sql_aggregation(
        self, input_value: DAGValue, agg_ops: Dict[str, tuple[str, str]]
    ) -> DAGValue:
        view_name = _register_table(self.table_env, input_value.data, "aggregate")
        select_exprs = [_quote_identifier(key) for key in self.group_keys]
        for alias, (function, column) in agg_ops.items():
            sql_function = {
                "mean": "AVG",
                "avg": "AVG",
                "sum": "SUM",
                "min": "MIN",
                "max": "MAX",
                "count": "COUNT",
                "nunique": "COUNT_DISTINCT",
                "std": "STDDEV_SAMP",
                "var": "VAR_SAMP",
            }.get(function, function.upper())
            if sql_function == "COUNT_DISTINCT":
                expr = (
                    f"COUNT(DISTINCT {_quote_identifier(column)}) "
                    f"AS {_quote_identifier(alias)}"
                )
            else:
                expr = (
                    f"{sql_function}({_quote_identifier(column)}) "
                    f"AS {_quote_identifier(alias)}"
                )
            select_exprs.append(expr)

        query = (
            f"SELECT {', '.join(select_exprs)} "
            f"FROM {_quote_identifier(view_name)} "
            f"GROUP BY {', '.join(_quote_identifier(key) for key in self.group_keys)}"
        )
        return _sql_value(
            self.table_env,
            query,
            [*self.group_keys, *agg_ops.keys()],
            metadata={"aggregated": True},
        )


class FlinkDedupNode(DAGNode):
    def __init__(
        self,
        name: str,
        column_info: ColumnInfo,
        table_env: Any,
        split_num: int,
        inputs: Optional[List[DAGNode]] = None,
    ) -> None:
        super().__init__(name, inputs=inputs)
        self.column_info = column_info
        self.table_env = table_env
        self.split_num = split_num

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.FLINK)

        _require_sql(self.table_env, self.name)
        return self._execute_sql_dedup(input_value)

    def _execute_sql_dedup(self, input_value: DAGValue) -> DAGValue:
        columns = _get_columns(input_value)
        dedup_keys = (
            [ENTITY_ROW_ID]
            if ENTITY_ROW_ID in columns
            else self.column_info.join_keys_columns
        )
        dedup_keys = [key for key in dedup_keys if key in columns]
        if not dedup_keys:
            return input_value

        order_columns = [
            self.column_info.timestamp_column,
            self.column_info.created_timestamp_column,
        ]
        order_exprs = [
            f"{_quote_identifier(column)} DESC"
            for column in order_columns
            if column and column in columns
        ]
        if not order_exprs:
            order_exprs = [f"{_quote_identifier(dedup_keys[0])} ASC"]

        view_name = _register_table(self.table_env, input_value.data, "dedup")
        select_columns = ", ".join(_quote_identifier(column) for column in columns)
        query = (
            f"SELECT {select_columns} FROM ("
            f"SELECT *, ROW_NUMBER() OVER ("
            f"PARTITION BY {', '.join(_quote_identifier(key) for key in dedup_keys)} "
            f"ORDER BY {', '.join(order_exprs)}"
            f") AS {_quote_identifier(DEDUP_ROW_NUMBER)} "
            f"FROM {_quote_identifier(view_name)}"
            f") WHERE {_quote_identifier(DEDUP_ROW_NUMBER)} = 1"
        )
        return _sql_value(
            self.table_env,
            query,
            columns,
            metadata={**(input_value.metadata or {}), "deduped": True},
        )


class FlinkTransformationNode(DAGNode):
    def __init__(
        self,
        name: str,
        transformation_fn: Callable[..., Any],
        table_env: Any,
        split_num: int,
        inputs: Optional[List[DAGNode]] = None,
    ) -> None:
        super().__init__(name, inputs=inputs)
        self.transformation_fn = transformation_fn
        self.table_env = table_env
        self.split_num = split_num

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_values = self.get_input_values(context)
        for value in input_values:
            value.assert_format(DAGFormat.FLINK)

        input_tables = [value.data for value in input_values]
        transformed = self.transformation_fn(*input_tables)

        columns = _get_columns_from_schema(transformed)
        if columns is None:
            raise TypeError(
                "Flink transformations must return a PyFlink Table with a schema."
            )

        return DAGValue(
            data=transformed,
            format=DAGFormat.FLINK,
            metadata={"transformed": True, "columns": columns or []},
        )


class FlinkValidationNode(DAGNode):
    def __init__(
        self,
        name: str,
        expected_columns: dict[str, Optional[pa.DataType]],
        json_columns: Optional[Set[str]],
        table_env: Any,
        split_num: int,
        inputs: Optional[List[DAGNode]] = None,
    ) -> None:
        super().__init__(name, inputs=inputs)
        self.expected_columns = expected_columns
        self.json_columns = json_columns or set()
        self.table_env = table_env
        self.split_num = split_num

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.FLINK)

        columns = _get_columns(input_value)
        missing = set(self.expected_columns.keys()) - set(columns)
        if missing:
            raise ValueError(
                f"[Validation: {self.name}] Missing expected columns: {missing}. "
                f"Actual columns: {sorted(columns)}"
            )
        if not self.json_columns:
            return DAGValue(
                data=input_value.data,
                format=DAGFormat.FLINK,
                metadata={**(input_value.metadata or {}), "validated": True},
            )

        raise NotImplementedError(
            "JSON value validation is not supported by FlinkComputeEngine without "
            "collecting data out of Flink. Validate JSON upstream in Flink SQL or "
            "disable JSON validation for this FeatureView."
        )


class FlinkOutputNode(DAGNode):
    def __init__(
        self,
        name: str,
        feature_view: Union[BatchFeatureView, FeatureView, StreamFeatureView],
        table_env: Any,
        split_num: int,
        write_output: bool,
        inputs: Optional[List[DAGNode]] = None,
    ) -> None:
        super().__init__(name, inputs=inputs)
        self.feature_view = feature_view
        self.table_env = table_env
        self.split_num = split_num
        self.write_output = write_output

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.FLINK)
        output_value = self._drop_internal_columns(input_value)
        output_table = output_value.data
        if not self.write_output:
            return output_value

        columns = _get_columns(output_value)
        batch_size = context.repo_config.materialization_config.online_write_batch_size
        if self.feature_view.online:
            join_key_to_value_type = {
                entity.name: entity.dtype.to_value_type()
                for entity in self.feature_view.entity_columns
            }
        else:
            join_key_to_value_type = {}

        for output_arrow in flink_table_to_arrow_batches(
            output_table,
            columns,
            batch_size,
        ):
            if output_arrow.num_rows == 0:
                continue

            if self.feature_view.online:
                arrow_batches = (
                    [output_arrow]
                    if batch_size is None
                    else output_arrow.to_batches(max_chunksize=batch_size)
                )
                for batch in arrow_batches:
                    rows_to_write = _convert_arrow_to_proto(
                        batch, self.feature_view, join_key_to_value_type
                    )
                    context.online_store.online_write_batch(
                        config=context.repo_config,
                        table=self.feature_view,
                        data=rows_to_write,
                        progress=lambda x: None,
                    )

            if self.feature_view.offline:
                context.offline_store.offline_write_batch(
                    config=context.repo_config,
                    feature_view=self.feature_view,
                    table=output_arrow,
                    progress=lambda x: None,
                )

        return output_value

    def _drop_internal_columns(self, input_value: DAGValue) -> DAGValue:
        columns = _get_columns(input_value)
        output_columns = [column for column in columns if column != ENTITY_ROW_ID]
        if output_columns == columns:
            return input_value

        _require_sql(self.table_env, self.name)
        view_name = _register_table(self.table_env, input_value.data, "output")
        query = (
            f"SELECT {', '.join(_quote_identifier(column) for column in output_columns)} "
            f"FROM {_quote_identifier(view_name)}"
        )
        return _sql_value(
            self.table_env,
            query,
            output_columns,
            metadata={**(input_value.metadata or {}), "output_cleaned": True},
        )
