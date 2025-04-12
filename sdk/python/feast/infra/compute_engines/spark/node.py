from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Union, cast

from pyspark.sql import DataFrame, SparkSession, Window
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
    _get_entity_schema,
)
from feast.infra.offline_stores.offline_utils import (
    infer_event_timestamp_from_entity_df,
)
from feast.utils import _get_column_names, _get_fields_with_aliases


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


class SparkMaterializationReadNode(DAGNode):
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


class SparkHistoricalRetrievalReadNode(DAGNode):
    def __init__(
        self, name: str, task: HistoricalRetrievalTask, spark_session: SparkSession
    ):
        super().__init__(name)
        self.task = task
        self.spark_session = spark_session

    def execute(self, context: ExecutionContext) -> DAGValue:
        """
        Read data from the offline store on the Spark engine.
        TODO: Some functionality is duplicated with SparkMaterializationReadNode and spark get_historical_features.
        Args:
            context: SparkExecutionContext
        Returns: DAGValue
        """
        fv = self.task.feature_view
        source = fv.batch_source
        entities = context.entity_defs

        (
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
        ) = _get_column_names(fv, entities)

        # TODO: Use pull_all_from_table_or_query when it supports not filtering by timestamp
        # retrieval_job = offline_store.pull_all_from_table_or_query(
        #     config=context.repo_config,
        #     data_source=source,
        #     join_key_columns=join_key_columns,
        #     feature_name_columns=feature_name_columns,
        #     timestamp_field=timestamp_field,
        #     start_date=min_ts,
        #     end_date=max_ts,
        # )
        # spark_df = cast(SparkRetrievalJob, retrieval_job).to_spark_df()

        columns = join_key_columns + feature_name_columns + [timestamp_field]
        if created_timestamp_column:
            columns.append(created_timestamp_column)

        (fields_with_aliases, aliases) = _get_fields_with_aliases(
            fields=columns,
            field_mappings=source.field_mapping,
        )
        fields_with_alias_string = ", ".join(fields_with_aliases)

        from_expression = source.get_table_query_string()

        query = f"""
            SELECT {fields_with_alias_string}
            FROM {from_expression}
        """
        spark_df = self.spark_session.sql(query)

        return DAGValue(
            data=spark_df,
            format=DAGFormat.SPARK,
            metadata={
                "source": "feature_view_batch_source",
                "timestamp_field": timestamp_field,
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
        input_value = self.get_single_input_value(context)
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
        spark_session: SparkSession,
    ):
        super().__init__(name)
        self.join_keys = join_keys
        self.add_input(feature_node)
        self.feature_view = feature_view
        self.spark_session = spark_session

    def execute(self, context: ExecutionContext) -> DAGValue:
        feature_value = self.get_single_input_value(context)
        feature_value.assert_format(DAGFormat.SPARK)
        feature_df = feature_value.data

        entity_df = context.entity_df
        assert entity_df is not None, "entity_df must be set in ExecutionContext"

        # Get timestamp fields from feature view
        join_keys, feature_cols, ts_col, created_ts_col = _get_column_names(
            self.feature_view, context.entity_defs
        )

        # Rename entity_df event_timestamp_col to match feature_df
        entity_schema = _get_entity_schema(
            spark_session=self.spark_session,
            entity_df=entity_df,
        )
        event_timestamp_col = infer_event_timestamp_from_entity_df(
            entity_schema=entity_schema,
        )
        entity_ts_alias = "__entity_event_timestamp"
        entity_df = self.spark_session.createDataFrame(entity_df).withColumnRenamed(
            event_timestamp_col, entity_ts_alias
        )

        # Perform left join + event timestamp filtering
        joined = feature_df.join(entity_df, on=join_keys, how="left")
        joined = joined.filter(F.col(ts_col) <= F.col(entity_ts_alias))

        # Optional TTL filter: feature.ts >= entity.event_timestamp - ttl
        if self.feature_view.ttl:
            ttl_seconds = int(self.feature_view.ttl.total_seconds())
            lower_bound = F.col(entity_ts_alias) - F.expr(
                f"INTERVAL {ttl_seconds} seconds"
            )
            joined = joined.filter(F.col(ts_col) >= lower_bound)

        # Dedup with row_number
        partition_cols = join_keys + [entity_ts_alias]
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
        spark_df: DataFrame = self.get_single_input_value(context).data
        spark_serialized_artifacts = _SparkSerializedArtifacts.serialize(
            feature_view=self.feature_view, repo_config=context.repo_config
        )

        # ✅ 1. Write to offline store (if enabled)
        if self.feature_view.offline:
            # TODO: Update _map_by_partition to be able to write to offline store
            pass

        # ✅ 2. Write to online store (if enabled)
        if self.feature_view.online:
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
        input_val = self.get_single_input_value(context)
        input_val.assert_format(DAGFormat.SPARK)

        transformed_df = self.udf(input_val.data)

        return DAGValue(
            data=transformed_df, format=DAGFormat.SPARK, metadata={"transformed": True}
        )
