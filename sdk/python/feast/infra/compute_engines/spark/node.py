from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union, cast

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from feast import BatchFeatureView, StreamFeatureView
from feast.aggregation import Aggregation
from feast.infra.compute_engines.base import HistoricalRetrievalTask
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.materialization.batch_materialization_engine import MaterializationTask
from feast.infra.materialization.contrib.spark.spark_materialization_engine import (
    _map_by_partition,
    _SparkSerializedArtifacts,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkRetrievalJob,
)
from feast.utils import _get_column_names


@dataclass
class SparkJoinContext:
    name: str  # feature view name or alias
    join_keys: List[str]
    feature_columns: List[str]
    timestamp_field: str
    created_timestamp_column: Optional[str]
    ttl_seconds: Optional[int]
    min_event_timestamp: Optional[datetime]
    max_event_timestamp: Optional[datetime]
    field_mapping: Dict[str, str]  # original_column_name -> renamed_column
    full_feature_names: bool = False  # apply feature view name prefix


class SparkReadNode(DAGNode):
    def __init__(
        self, name: str, task: Union[MaterializationTask, HistoricalRetrievalTask]
    ):
        super().__init__(name)
        self.task = task

    def execute(self, context: ExecutionContext) -> DAGValue:
        offline_store = context.offline_store
        start_time = self.task.start_time
        end_time = self.task.end_time

        (
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
        ) = _get_column_names(self.task.feature_view, context.entity_defs)

        # 📥 Reuse Feast's robust query resolver
        retrieval_job = offline_store.pull_latest_from_table_or_query(
            config=context.repo_config,
            data_source=self.task.feature_view.batch_source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=start_time,
            end_date=end_time,
        )
        spark_df = cast(SparkRetrievalJob, retrieval_job).to_spark_df()

        return DAGValue(
            data=spark_df,
            format=DAGFormat.SPARK,
            metadata={
                "source": "feature_view_batch_source",
                "timestamp_field": timestamp_field,
                "created_timestamp_column": created_timestamp_column,
                "start_date": start_time,
                "end_date": end_time,
            },
        )


class SparkAggregationNode(DAGNode):
    def __init__(
        self,
        name: str,
        input_node: DAGNode,
        aggregations: List[Aggregation],
        group_by_keys: List[str],
        timestamp_col: str,
    ):
        super().__init__(name)
        self.add_input(input_node)
        self.aggregations = aggregations
        self.group_by_keys = group_by_keys
        self.timestamp_col = timestamp_col

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = context.node_outputs[self.inputs[0].name]
        input_value.assert_format(DAGFormat.SPARK)
        input_df: DataFrame = input_value.data

        agg_exprs = []
        for agg in self.aggregations:
            func = getattr(F, agg.function)
            expr = func(agg.column).alias(
                f"{agg.function}_{agg.column}_{int(agg.time_window.total_seconds())}s"
                if agg.time_window
                else f"{agg.function}_{agg.column}"
            )
            agg_exprs.append(expr)

        if any(agg.time_window for agg in self.aggregations):
            # 🕒 Use Spark's `window` function
            time_window = self.aggregations[
                0
            ].time_window  # assume consistent window size for now
            if time_window is None:
                raise ValueError("Aggregation requires time_window but got None.")
            window_duration_str = f"{int(time_window.total_seconds())} seconds"

            grouped = input_df.groupBy(
                *self.group_by_keys,
                F.window(F.col(self.timestamp_col), window_duration_str),
            ).agg(*agg_exprs)
        else:
            # Simple aggregation
            grouped = input_df.groupBy(*self.group_by_keys).agg(*agg_exprs)

        return DAGValue(
            data=grouped, format=DAGFormat.SPARK, metadata={"aggregated": True}
        )


class SparkJoinNode(DAGNode):
    def __init__(
        self,
        name: str,
        feature_node: DAGNode,
        join_keys: List[str],
        feature_view: Union[BatchFeatureView, StreamFeatureView],
    ):
        super().__init__(name)
        self.join_keys = join_keys
        self.add_input(feature_node)
        self.feature_view = feature_view

    def execute(self, context: ExecutionContext) -> DAGValue:
        feature_value = context.node_outputs[self.inputs[1].name]
        feature_value.assert_format(DAGFormat.SPARK)

        entity_df = context.entity_df
        feature_df = feature_value.data

        # Get timestamp fields from feature view
        join_keys, feature_cols, ts_col, created_ts_col = _get_column_names(
            self.feature_view, context.entity_defs
        )

        entity_event_ts_col = "event_timestamp"  # Standardized by SparkEntityLoadNode

        # Perform left join + event timestamp filtering
        joined = feature_df.join(entity_df, on=join_keys, how="left")
        joined = joined.filter(F.col(ts_col) <= F.col(entity_event_ts_col))

        # Dedup with row_number
        partition_cols = join_keys + [entity_event_ts_col]
        ordering = [F.col(ts_col).desc()]
        if created_ts_col:
            ordering.append(F.col(created_ts_col).desc())

        window = Window.partitionBy(*partition_cols).orderBy(*ordering)
        deduped = (
            joined.withColumn("row_num", F.row_number().over(window))
            .filter("row_num = 1")
            .drop("row_num")
        )

        return DAGValue(
            data=deduped, format=DAGFormat.SPARK, metadata={"joined_on": join_keys}
        )


class SparkWriteNode(DAGNode):
    def __init__(
        self,
        name: str,
        input_node: DAGNode,
        feature_view: Union[BatchFeatureView, StreamFeatureView],
    ):
        super().__init__(name)
        self.add_input(input_node)
        self.feature_view = feature_view

    def execute(self, context: ExecutionContext) -> DAGValue:
        spark_df: DataFrame = context.node_outputs[self.inputs[0].name].data

        # ✅ 1. Write to offline store (if enabled)
        if self.feature_view.online:
            context.offline_store.offline_write_batch(
                config=context.repo_config,
                feature_view=self.feature_view,
                table=spark_df,
                progress=None,
            )

        # ✅ 2. Write to online store (if enabled)
        if self.feature_view.offline:
            spark_serialized_artifacts = _SparkSerializedArtifacts.serialize(
                feature_view=self.feature_view, repo_config=context.repo_config
            )
            spark_df.mapInPandas(
                lambda x: _map_by_partition(x, spark_serialized_artifacts), "status int"
            ).count()

        return DAGValue(
            data=spark_df,
            format=DAGFormat.SPARK,
            metadata={
                "feature_view": self.feature_view.name,
                "write_to_online": self.feature_view.online,
                "write_to_offline": self.feature_view.offline,
            },
        )


class SparkTransformationNode(DAGNode):
    def __init__(self, name: str, input_node: DAGNode, udf):
        super().__init__(name)
        self.add_input(input_node)
        self.udf = udf

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_val = context.node_outputs[self.inputs[0].name]
        input_val.assert_format(DAGFormat.SPARK)

        transformed_df = self.udf(input_val.data)

        return DAGValue(
            data=transformed_df, format=DAGFormat.SPARK, metadata={"transformed": True}
        )
