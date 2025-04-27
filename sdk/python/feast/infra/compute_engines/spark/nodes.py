from datetime import datetime, timedelta
from typing import List, Optional, Union, cast

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from feast import BatchFeatureView, StreamFeatureView
from feast.aggregation import Aggregation
from feast.data_source import DataSource
from feast.infra.common.serde import SerializedArtifacts
from feast.infra.compute_engines.dag.context import ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.spark.utils import map_in_arrow
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkRetrievalJob,
    _get_entity_schema,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.infra.offline_stores.offline_utils import (
    infer_event_timestamp_from_entity_df,
)

ENTITY_TS_ALIAS = "__entity_event_timestamp"


# Rename entity_df event_timestamp_col to match feature_df
def rename_entity_ts_column(
    spark_session: SparkSession, entity_df: DataFrame
) -> DataFrame:
    # check if entity_ts_alias already exists
    if ENTITY_TS_ALIAS in entity_df.columns:
        return entity_df

    entity_schema = _get_entity_schema(
        spark_session=spark_session,
        entity_df=entity_df,
    )
    event_timestamp_col = infer_event_timestamp_from_entity_df(
        entity_schema=entity_schema,
    )
    if not isinstance(entity_df, DataFrame):
        entity_df = spark_session.createDataFrame(entity_df)
    entity_df = entity_df.withColumnRenamed(event_timestamp_col, ENTITY_TS_ALIAS)
    return entity_df


class SparkReadNode(DAGNode):
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

    def execute(self, context: ExecutionContext) -> DAGValue:
        offline_store = context.offline_store
        (
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
        ) = context.column_info

        # 📥 Reuse Feast's robust query resolver
        retrieval_job = offline_store.pull_all_from_table_or_query(
            config=context.repo_config,
            data_source=self.source,
            join_key_columns=join_key_columns,
            feature_name_columns=feature_name_columns,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            start_date=self.start_time,
            end_date=self.end_time,
        )
        spark_df = cast(SparkRetrievalJob, retrieval_job).to_spark_df()

        return DAGValue(
            data=spark_df,
            format=DAGFormat.SPARK,
            metadata={
                "source": "feature_view_batch_source",
                "timestamp_field": timestamp_field,
                "created_timestamp_column": created_timestamp_column,
                "start_date": self.start_time,
                "end_date": self.end_time,
            },
        )


class SparkAggregationNode(DAGNode):
    def __init__(
        self,
        name: str,
        aggregations: List[Aggregation],
        group_by_keys: List[str],
        timestamp_col: str,
    ):
        super().__init__(name)
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
            grouped = input_df.groupBy(
                *self.group_by_keys,
            ).agg(*agg_exprs)

        return DAGValue(
            data=grouped, format=DAGFormat.SPARK, metadata={"aggregated": True}
        )


class SparkJoinNode(DAGNode):
    def __init__(
        self,
        name: str,
        spark_session: SparkSession,
    ):
        super().__init__(name)
        self.spark_session = spark_session

    def execute(self, context: ExecutionContext) -> DAGValue:
        feature_value = self.get_single_input_value(context)
        feature_value.assert_format(DAGFormat.SPARK)
        feature_df: DataFrame = feature_value.data

        entity_df = context.entity_df
        if entity_df is None:
            return DAGValue(
                data=feature_df,
                format=DAGFormat.SPARK,
                metadata={"joined_on": None},
            )

        # Get timestamp fields from feature view
        join_keys, feature_cols, ts_col, created_ts_col = context.column_info

        # Rename entity_df event_timestamp_col to match feature_df
        entity_df = rename_entity_ts_column(
            spark_session=self.spark_session,
            entity_df=entity_df,
        )

        # Perform left join on entity df
        joined = feature_df.join(entity_df, on=join_keys, how="left")

        return DAGValue(
            data=joined, format=DAGFormat.SPARK, metadata={"joined_on": join_keys}
        )


class SparkFilterNode(DAGNode):
    def __init__(
        self,
        name: str,
        spark_session: SparkSession,
        ttl: Optional[timedelta] = None,
        filter_condition: Optional[str] = None,
    ):
        super().__init__(name)
        self.spark_session = spark_session
        self.ttl = ttl
        self.filter_condition = filter_condition

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.SPARK)
        input_df: DataFrame = input_value.data

        # Get timestamp fields from feature view
        _, _, ts_col, _ = context.column_info

        # Optional filter: feature.ts <= entity.event_timestamp
        filtered_df = input_df
        if ENTITY_TS_ALIAS in input_df.columns:
            filtered_df = filtered_df.filter(F.col(ts_col) <= F.col(ENTITY_TS_ALIAS))

            # Optional TTL filter: feature.ts >= entity.event_timestamp - ttl
            if self.ttl:
                ttl_seconds = int(self.ttl.total_seconds())
                lower_bound = F.col(ENTITY_TS_ALIAS) - F.expr(
                    f"INTERVAL {ttl_seconds} seconds"
                )
                filtered_df = filtered_df.filter(F.col(ts_col) >= lower_bound)

        # Optional custom filter condition
        if self.filter_condition:
            filtered_df = filtered_df.filter(self.filter_condition)

        return DAGValue(
            data=filtered_df,
            format=DAGFormat.SPARK,
            metadata={"filter_applied": True},
        )


class SparkDedupNode(DAGNode):
    def __init__(
        self,
        name: str,
        spark_session: SparkSession,
    ):
        super().__init__(name)
        self.spark_session = spark_session

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.SPARK)
        input_df: DataFrame = input_value.data

        # Get timestamp fields from feature view
        join_keys, _, ts_col, created_ts_col = context.column_info

        # Dedup based on join keys and event timestamp
        # Dedup with row_number
        partition_cols = join_keys + [ENTITY_TS_ALIAS]
        ordering = [F.col(ts_col).desc()]
        if created_ts_col:
            ordering.append(F.col(created_ts_col).desc())

        window = Window.partitionBy(*partition_cols).orderBy(*ordering)
        deduped_df = (
            input_df.withColumn("row_num", F.row_number().over(window))
            .filter("row_num = 1")
            .drop("row_num")
        )

        return DAGValue(
            data=deduped_df,
            format=DAGFormat.SPARK,
            metadata={"deduped": True},
        )


class SparkWriteNode(DAGNode):
    def __init__(
        self,
        name: str,
        feature_view: Union[BatchFeatureView, StreamFeatureView],
    ):
        super().__init__(name)
        self.feature_view = feature_view

    def execute(self, context: ExecutionContext) -> DAGValue:
        spark_df: DataFrame = self.get_single_input_value(context).data
        serialized_artifacts = SerializedArtifacts.serialize(
            feature_view=self.feature_view, repo_config=context.repo_config
        )

        # ✅ 1. Write to online store if online enabled
        if self.feature_view.online:
            spark_df.mapInArrow(
                lambda x: map_in_arrow(x, serialized_artifacts, mode="online"),
                spark_df.schema,
            ).count()

        # ✅ 2. Write to offline store if offline enabled
        if self.feature_view.offline:
            if not isinstance(self.feature_view.batch_source, SparkSource):
                spark_df.mapInArrow(
                    lambda x: map_in_arrow(x, serialized_artifacts, mode="offline"),
                    spark_df.schema,
                ).count()
            # Directly write spark df to spark offline store without using mapInArrow
            else:
                dest_path = self.feature_view.batch_source.path
                file_format = self.feature_view.batch_source.file_format
                if not dest_path or not file_format:
                    raise ValueError(
                        "Destination path and file format must be specified for SparkSource."
                    )
                spark_df.write.format(file_format).mode("append").save(dest_path)

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
    def __init__(self, name: str, udf):
        super().__init__(name)
        self.udf = udf

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_val = self.get_single_input_value(context)
        input_val.assert_format(DAGFormat.SPARK)

        transformed_df = self.udf(input_val.data)

        return DAGValue(
            data=transformed_df, format=DAGFormat.SPARK, metadata={"transformed": True}
        )
