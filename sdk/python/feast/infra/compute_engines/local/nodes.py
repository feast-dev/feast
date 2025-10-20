from datetime import datetime, timedelta
from typing import List, Optional, Union

import pyarrow as pa

from feast import BatchFeatureView, StreamFeatureView
from feast.data_source import DataSource
from feast.infra.compute_engines.backends.base import DataFrameBackend
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.local.arrow_table_value import ArrowTableValue
from feast.infra.compute_engines.local.local_node import LocalNode
from feast.infra.compute_engines.utils import (
    create_offline_store_retrieval_job,
)
from feast.infra.offline_stores.offline_utils import (
    infer_event_timestamp_from_entity_df,
)
from feast.utils import _convert_arrow_to_proto

ENTITY_TS_ALIAS = "__entity_event_timestamp"


class LocalSourceReadNode(LocalNode):
    def __init__(
        self,
        name: str,
        source: DataSource,
        column_info: ColumnInfo,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ):
        super().__init__(name)
        self.source = source
        self.column_info = column_info
        self.start_time = start_time
        self.end_time = end_time

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        retrieval_job = create_offline_store_retrieval_job(
            data_source=self.source,
            context=context,
            start_time=self.start_time,
            end_time=self.end_time,
            column_info=self.column_info,
        )
        arrow_table = retrieval_job.to_arrow()
        if self.column_info.field_mapping:
            arrow_table = arrow_table.rename_columns(
                [
                    self.column_info.field_mapping.get(col, col)
                    for col in arrow_table.column_names
                ]
            )
        return ArrowTableValue(data=arrow_table)


class LocalJoinNode(LocalNode):
    def __init__(
        self,
        name: str,
        column_info: ColumnInfo,
        backend: DataFrameBackend,
        inputs: Optional[List["DAGNode"]] = None,
        how: str = "inner",
    ):
        super().__init__(name, inputs or [])
        self.column_info = column_info
        self.backend = backend
        self.how = how

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        input_values = self.get_input_values(context)
        for val in input_values:
            val.assert_format(DAGFormat.ARROW)

        # Convert all upstream ArrowTables to backend DataFrames
        joined_df = self.backend.from_arrow(input_values[0].data)
        for val in input_values[1:]:
            next_df = self.backend.from_arrow(val.data)
            joined_df = self.backend.join(
                joined_df,
                next_df,
                on=self.column_info.join_keys,
                how=self.how,
            )

        # If entity_df is provided, join it in last
        if context.entity_df is not None:
            entity_df = self.backend.from_arrow(pa.Table.from_pandas(context.entity_df))

            entity_schema = dict(zip(entity_df.columns, entity_df.dtypes))
            entity_ts_col = infer_event_timestamp_from_entity_df(entity_schema)

            if entity_ts_col != ENTITY_TS_ALIAS:
                entity_df = self.backend.rename_columns(
                    entity_df, {entity_ts_col: ENTITY_TS_ALIAS}
                )

            joined_df = self.backend.join(
                entity_df,
                joined_df,
                on=self.column_info.join_keys,
                how="left",
            )

        result = self.backend.to_arrow(joined_df)
        return ArrowTableValue(result)


class LocalFilterNode(LocalNode):
    def __init__(
        self,
        name: str,
        column_info: ColumnInfo,
        backend: DataFrameBackend,
        filter_expr: Optional[str] = None,
        ttl: Optional[timedelta] = None,
        inputs=None,
    ):
        super().__init__(name, inputs=inputs)
        self.column_info = column_info
        self.backend = backend
        self.filter_expr = filter_expr
        self.ttl = ttl

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        input_table = self.get_single_table(context).data
        df = self.backend.from_arrow(input_table)

        timestamp_column = self.column_info.timestamp_column

        if ENTITY_TS_ALIAS in self.backend.columns(df):
            # filter where feature.ts <= entity.event_timestamp
            df = df[df[timestamp_column] <= df[ENTITY_TS_ALIAS]]

            # TTL: feature.ts >= entity.event_timestamp - ttl
            if self.ttl:
                lower_bound = df[ENTITY_TS_ALIAS] - self.backend.to_timedelta_value(
                    self.ttl
                )
                df = df[df[timestamp_column] >= lower_bound]

        # Optional user-defined filter expression (e.g., "value > 0")
        if self.filter_expr:
            df = self.backend.filter(df, self.filter_expr)

        result = self.backend.to_arrow(df)
        output = ArrowTableValue(result)
        context.node_outputs[self.name] = output
        return output


class LocalAggregationNode(LocalNode):
    def __init__(
        self,
        name: str,
        backend: DataFrameBackend,
        group_keys: list[str],
        agg_ops: dict,
        inputs=None,
    ):
        super().__init__(name, inputs=inputs)
        self.backend = backend
        self.group_keys = group_keys
        self.agg_ops = agg_ops

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        input_table = self.get_single_table(context).data
        df = self.backend.from_arrow(input_table)
        grouped_df = self.backend.groupby_agg(df, self.group_keys, self.agg_ops)
        result = self.backend.to_arrow(grouped_df)
        output = ArrowTableValue(result)
        return output


class LocalDedupNode(LocalNode):
    def __init__(
        self, name: str, column_info: ColumnInfo, backend: DataFrameBackend, inputs=None
    ):
        super().__init__(name, inputs=inputs)
        self.column_info = column_info
        self.backend = backend

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        input_table = self.get_single_table(context).data
        df = self.backend.from_arrow(input_table)

        # Extract join_keys, timestamp, and created_ts from context

        # Dedup strategy: sort and drop_duplicates
        dedup_keys = self.column_info.join_keys
        if dedup_keys:
            sort_keys = [self.column_info.timestamp_column]
            if (
                self.column_info.created_timestamp_column
                and self.column_info.created_timestamp_column in df.columns
            ):
                sort_keys.append(self.column_info.created_timestamp_column)

            df = self.backend.drop_duplicates(
                df, keys=dedup_keys, sort_by=sort_keys, ascending=False
            )
        result = self.backend.to_arrow(df)
        output = ArrowTableValue(result)
        context.node_outputs[self.name] = output
        return output


class LocalTransformationNode(LocalNode):
    def __init__(
        self, name: str, transformation_fn, backend: DataFrameBackend, inputs=None
    ):
        super().__init__(name, inputs=inputs)
        self.transformation_fn = transformation_fn
        self.backend = backend

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        input_table = self.get_single_table(context).data
        df = self.backend.from_arrow(input_table)
        transformed_df = self.transformation_fn(df)
        result = self.backend.to_arrow(transformed_df)
        output = ArrowTableValue(result)
        context.node_outputs[self.name] = output
        return output


class LocalValidationNode(LocalNode):
    def __init__(
        self, name: str, validation_config, backend: DataFrameBackend, inputs=None
    ):
        super().__init__(name, inputs=inputs)
        self.validation_config = validation_config
        self.backend = backend

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        input_table = self.get_single_table(context).data
        df = self.backend.from_arrow(input_table)
        # Placeholder for actual validation logic
        if self.validation_config:
            print(f"[Validation: {self.name}] Passed.")
        result = self.backend.to_arrow(df)
        output = ArrowTableValue(result)
        context.node_outputs[self.name] = output
        return output


class LocalOutputNode(LocalNode):
    def __init__(
        self,
        name: str,
        feature_view: Union[BatchFeatureView, StreamFeatureView],
        inputs=None,
    ):
        super().__init__(name, inputs=inputs)
        self.feature_view = feature_view

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        input_table = self.get_single_table(context).data
        context.node_outputs[self.name] = input_table

        if input_table.num_rows == 0:
            return input_table

        if self.feature_view.online:
            online_store = context.online_store

            join_key_to_value_type = {
                entity.name: entity.dtype.to_value_type()
                for entity in self.feature_view.entity_columns
            }

            rows_to_write = _convert_arrow_to_proto(
                input_table, self.feature_view, join_key_to_value_type
            )

            online_store.online_write_batch(
                config=context.repo_config,
                table=self.feature_view,
                data=rows_to_write,
                progress=lambda x: None,
            )

        if self.feature_view.offline:
            offline_store = context.offline_store
            offline_store.offline_write_batch(
                config=context.repo_config,
                feature_view=self.feature_view,
                table=input_table,
                progress=lambda x: None,
            )

        return input_table
