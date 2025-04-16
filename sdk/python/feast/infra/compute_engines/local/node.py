from datetime import timedelta
from typing import Optional

import pyarrow as pa

from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.local.arrow_table_value import ArrowTableValue
from feast.infra.compute_engines.local.backends.base import DataFrameBackend
from feast.infra.compute_engines.local.local_node import LocalNode

ENTITY_TS_ALIAS = "__entity_event_timestamp"


class LocalSourceReadNode(LocalNode):
    def __init__(self, name: str, feature_view, task):
        super().__init__(name)
        self.feature_view = feature_view
        self.task = task

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        # TODO : Implement the logic to read from offline store
        return ArrowTableValue(data=pa.Table.from_pandas(context.entity_df))


class LocalJoinNode(LocalNode):
    def __init__(self, name: str, backend: DataFrameBackend):
        super().__init__(name)
        self.backend = backend

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        feature_table = self.get_single_table(context).data
        entity_table = pa.Table(context.entity_df)
        feature_df = self.backend.from_arrow(feature_table)
        entity_df = self.backend.from_arrow(entity_table)

        join_keys, feature_cols, ts_col, created_ts_col = context.column_info

        # Rename entity timestamp if needed
        if ENTITY_TS_ALIAS in entity_df.columns and ENTITY_TS_ALIAS != ts_col:
            entity_df = entity_df.rename(columns={ENTITY_TS_ALIAS: ts_col})
        joined_df = self.backend.join(feature_df, entity_df, on=join_keys, how="left")
        result = self.backend.to_arrow(joined_df)
        output = ArrowTableValue(result)
        context.node_outputs[self.name] = output
        return output


class LocalFilterNode(LocalNode):
    def __init__(
        self,
        name: str,
        backend: DataFrameBackend,
        filter_expr: Optional[str] = None,
        ttl: Optional[timedelta] = None,
    ):
        super().__init__(name)
        self.backend = backend
        self.filter_expr = filter_expr
        self.ttl = ttl  # in seconds

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        input_table = self.get_single_table(context).data
        df = self.backend.from_arrow(input_table)

        _, _, ts_col, _ = context.column_info

        if ENTITY_TS_ALIAS in self.backend.columns(df):
            # filter where feature.ts <= entity.event_timestamp
            df = df[df[ts_col] <= df[ENTITY_TS_ALIAS]]

            # TTL: feature.ts >= entity.event_timestamp - ttl
            if self.ttl:
                lower_bound = df[ENTITY_TS_ALIAS] - self.backend.to_timedelta_value(
                    self.ttl
                )
                df = df[df[ts_col] >= lower_bound]

        # Optional user-defined filter expression (e.g., "value > 0")
        if self.filter_expr:
            df = self.backend.filter(df, self.filter_expr)

        result = self.backend.to_arrow(df)
        output = ArrowTableValue(result)
        context.node_outputs[self.name] = output
        return output


class LocalAggregationNode(LocalNode):
    def __init__(self, name: str, group_keys: list[str], agg_ops: dict, backend):
        super().__init__(name)
        self.group_keys = group_keys
        self.agg_ops = agg_ops
        self.backend = backend

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        input_table = self.get_single_table(context).data
        df = self.backend.from_arrow(input_table)
        grouped_df = self.backend.groupby_agg(df, self.group_keys, self.agg_ops)
        result = self.backend.to_arrow(grouped_df)
        output = ArrowTableValue(result)
        context.node_outputs[self.name] = output
        return output


class LocalDedupNode(LocalNode):
    def __init__(self, name: str, backend: DataFrameBackend):
        super().__init__(name)
        self.backend = backend

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        input_table = self.get_single_table(context).data
        df = self.backend.from_arrow(input_table)

        # Extract join_keys, timestamp, and created_ts from context
        join_keys, _, ts_col, created_ts_col = context.column_info

        # Dedup strategy: sort and drop_duplicates
        sort_keys = [ts_col]
        if created_ts_col:
            sort_keys.append(created_ts_col)

        dedup_keys = join_keys + [ENTITY_TS_ALIAS]
        df = self.backend.drop_duplicates(
            df, keys=dedup_keys, sort_by=sort_keys, ascending=False
        )
        result = self.backend.to_arrow(df)
        output = ArrowTableValue(result)
        context.node_outputs[self.name] = output
        return output


class LocalTransformationNode(LocalNode):
    def __init__(self, name: str, transformation_fn, backend):
        super().__init__(name)
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
    def __init__(self, name: str, validation_config, backend):
        super().__init__(name)
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
    def __init__(self, name: str):
        super().__init__(name)

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        input_table = self.get_single_table(context).data
        context.node_outputs[self.name] = input_table
        return input_table
