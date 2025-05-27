from datetime import datetime, timedelta
from typing import Optional, Union

import pyarrow as pa

from feast import BatchFeatureView, StreamFeatureView
from feast.data_source import DataSource
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.local.arrow_table_value import ArrowTableValue
from feast.infra.compute_engines.local.backends.base import DataFrameBackend
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
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ):
        super().__init__(name)
        self.source = source
        self.start_time = start_time
        self.end_time = end_time

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        retrieval_job = create_offline_store_retrieval_job(
            data_source=self.source,
            context=context,
            start_time=self.start_time,
            end_time=self.end_time,
        )
        arrow_table = retrieval_job.to_arrow()
        return ArrowTableValue(data=arrow_table)


class LocalJoinNode(LocalNode):
    def __init__(self, name: str, backend: DataFrameBackend):
        super().__init__(name)
        self.backend = backend

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        feature_table = self.get_single_table(context).data

        if context.entity_df is None:
            output = ArrowTableValue(feature_table)
            context.node_outputs[self.name] = output
            return output

        entity_table = pa.Table.from_pandas(context.entity_df)
        feature_df = self.backend.from_arrow(feature_table)
        entity_df = self.backend.from_arrow(entity_table)

        entity_schema = dict(zip(entity_df.columns, entity_df.dtypes))
        entity_df_event_timestamp_col = infer_event_timestamp_from_entity_df(
            entity_schema
        )

        column_info = context.column_info

        entity_df = self.backend.rename_columns(
            entity_df, {entity_df_event_timestamp_col: ENTITY_TS_ALIAS}
        )

        joined_df = self.backend.join(
            feature_df, entity_df, on=column_info.join_keys, how="left"
        )
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
        self.ttl = ttl

    def execute(self, context: ExecutionContext) -> ArrowTableValue:
        input_table = self.get_single_table(context).data
        df = self.backend.from_arrow(input_table)

        timestamp_column = context.column_info.timestamp_column

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
        self, name: str, backend: DataFrameBackend, group_keys: list[str], agg_ops: dict
    ):
        super().__init__(name)
        self.backend = backend
        self.group_keys = group_keys
        self.agg_ops = agg_ops

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
        column_info = context.column_info

        # Dedup strategy: sort and drop_duplicates
        dedup_keys = context.column_info.join_keys
        if dedup_keys:
            sort_keys = [column_info.timestamp_column]
            if column_info.created_timestamp_column:
                sort_keys.append(column_info.created_timestamp_column)

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
    def __init__(
        self, name: str, feature_view: Union[BatchFeatureView, StreamFeatureView]
    ):
        super().__init__(name)
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
