import json
import logging
from datetime import datetime, timedelta
from typing import Callable, Dict, List, Optional, Set, Union, cast

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.pandas.types import from_arrow_schema
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructType,
    TimestampType,
)
from pyspark.sql.types import (
    DataType as SparkDataType,
)

from feast import BatchFeatureView, StreamFeatureView
from feast.aggregation import Aggregation
from feast.data_source import DataSource
from feast.infra.common.serde import SerializedArtifacts
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.spark.utils import map_in_arrow
from feast.infra.compute_engines.utils import (
    create_offline_store_retrieval_job,
)
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

logger = logging.getLogger(__name__)


def from_feast_to_spark_type(feast_type) -> Optional[SparkDataType]:
    """Convert a Feast type to a PySpark DataType.

    Returns None if the Feast type cannot be mapped.
    """
    from feast.types import (
        Array,
        PrimitiveFeastType,
        Set,
        Struct,
    )

    if isinstance(feast_type, Struct):
        from pyspark.sql.types import StructField

        spark_fields = []
        for name, ftype in feast_type.fields.items():
            spark_type = from_feast_to_spark_type(ftype)
            if spark_type is None:
                return None
            spark_fields.append(StructField(name, spark_type, nullable=True))
        return StructType(spark_fields)

    if isinstance(feast_type, PrimitiveFeastType):
        mapping = {
            PrimitiveFeastType.BYTES: BinaryType(),
            PrimitiveFeastType.STRING: StringType(),
            PrimitiveFeastType.INT32: IntegerType(),
            PrimitiveFeastType.INT64: LongType(),
            PrimitiveFeastType.FLOAT64: DoubleType(),
            PrimitiveFeastType.FLOAT32: FloatType(),
            PrimitiveFeastType.BOOL: BooleanType(),
            PrimitiveFeastType.UNIX_TIMESTAMP: TimestampType(),
            PrimitiveFeastType.MAP: MapType(StringType(), StringType()),
            PrimitiveFeastType.JSON: StringType(),
        }
        return mapping.get(feast_type)

    if isinstance(feast_type, Array):
        base_type = feast_type.base_type
        if isinstance(base_type, Struct):
            inner = from_feast_to_spark_type(base_type)
            return ArrayType(inner) if inner else None
        if isinstance(base_type, PrimitiveFeastType):
            if base_type == PrimitiveFeastType.MAP:
                return ArrayType(MapType(StringType(), StringType()))
            inner = from_feast_to_spark_type(base_type)
            return ArrayType(inner) if inner else None

    if isinstance(feast_type, Set):
        inner = from_feast_to_spark_type(feast_type.base_type)
        return ArrayType(inner) if inner else None

    return None


def _spark_types_compatible(expected: SparkDataType, actual: SparkDataType) -> bool:
    """Check if two Spark types are compatible for validation purposes.

    Exact match is always compatible. Beyond that, we allow common
    representations that arise from different data source encodings.
    """
    if expected == actual:
        return True

    # Map ↔ Struct: data sources may encode maps as structs or vice versa
    if isinstance(expected, MapType) and isinstance(actual, (MapType, StructType)):
        return True
    if isinstance(expected, StructType) and isinstance(actual, (StructType, MapType)):
        return True

    # Json (StringType) is always compatible with StringType
    if isinstance(expected, StringType) and isinstance(actual, StringType):
        return True

    # Integer widening: IntegerType ↔ LongType
    if isinstance(expected, (IntegerType, LongType)) and isinstance(
        actual, (IntegerType, LongType)
    ):
        return True

    # Float widening: FloatType ↔ DoubleType
    if isinstance(expected, (FloatType, DoubleType)) and isinstance(
        actual, (FloatType, DoubleType)
    ):
        return True

    # Array compatibility: compare element types
    if isinstance(expected, ArrayType) and isinstance(actual, ArrayType):
        return _spark_types_compatible(expected.elementType, actual.elementType)

    return False


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
        column_info: ColumnInfo,
        spark_session: SparkSession,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ):
        super().__init__(name)
        self.source = source
        self.column_info = column_info
        self.spark_session = spark_session
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
        if isinstance(retrieval_job, SparkRetrievalJob):
            spark_df = cast(SparkRetrievalJob, retrieval_job).to_spark_df()
        else:
            arrow_table = retrieval_job.to_arrow()
            if arrow_table.num_rows == 0:
                spark_schema = from_arrow_schema(arrow_table.schema)
                spark_df = self.spark_session.createDataFrame(
                    self.spark_session.sparkContext.emptyRDD(), schema=spark_schema
                )
            else:
                spark_df = self.spark_session.createDataFrame(arrow_table.to_pandas())

        return DAGValue(
            data=spark_df,
            format=DAGFormat.SPARK,
            metadata={
                "source": "feature_view_batch_source",
                "timestamp_field": self.column_info.timestamp_column,
                "created_timestamp_column": self.column_info.created_timestamp_column,
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
        spark_session: SparkSession,
        inputs=None,
        enable_tiling: bool = False,
        hop_size: Optional[timedelta] = None,
    ):
        super().__init__(name, inputs=inputs)
        self.aggregations = aggregations
        self.group_by_keys = group_by_keys
        self.timestamp_col = timestamp_col
        self.spark_session = spark_session
        self.enable_tiling = enable_tiling
        self.hop_size = hop_size

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.SPARK)
        input_df: DataFrame = input_value.data

        # Check if tiling is enabled and we have time-windowed aggregations
        has_time_windows = any(agg.time_window for agg in self.aggregations)

        if self.enable_tiling and has_time_windows:
            return self._execute_tiled_aggregation(input_df)
        else:
            return self._execute_standard_aggregation(input_df)

    def _execute_tiled_aggregation(self, input_df: DataFrame) -> DAGValue:
        """
        Execute aggregation using tiling.
        """
        entity_keys = self.group_by_keys

        # Group aggregations by time window to process separately
        from collections import defaultdict

        aggs_by_window = defaultdict(list)
        for agg in self.aggregations:
            if agg.time_window is None:
                raise ValueError(
                    f"Tiling is enabled but aggregation on column '{agg.column}' has no time_window set. "
                    f"Either set time_window for all aggregations or disable tiling by setting enable_tiling=False."
                )
            aggs_by_window[agg.time_window].append(agg)

        from feast.aggregation.tiling.orchestrator import apply_sawtooth_window_tiling
        from feast.aggregation.tiling.tile_subtraction import (
            convert_cumulative_to_windowed,
            deduplicate_keep_latest,
        )

        input_pdf = input_df.toPandas()

        # Process each time window in pandas
        windowed_pdfs = []
        for time_window, window_aggs in aggs_by_window.items():
            # Step 1: Generate cumulative tiles
            tiles_pdf = apply_sawtooth_window_tiling(
                df=input_pdf,
                aggregations=window_aggs,
                group_by_keys=entity_keys,
                timestamp_col=self.timestamp_col,
                window_size=time_window,
                hop_size=self.hop_size or timedelta(minutes=5),
            )

            if tiles_pdf.empty:
                continue

            # Step 2: Convert to windowed aggregations
            windowed_pdf = convert_cumulative_to_windowed(
                tiles_df=tiles_pdf,
                entity_keys=entity_keys,
                timestamp_col=self.timestamp_col,
                window_size=time_window,
                aggregations=window_aggs,
            )

            if not windowed_pdf.empty:
                windowed_pdfs.append(windowed_pdf)

        if not windowed_pdfs:
            # No results, return empty Spark DataFrame with correct schema
            # Build expected columns: entity_keys + timestamp_col + feature columns
            expected_columns = entity_keys + [self.timestamp_col]
            for time_window, window_aggs in aggs_by_window.items():
                for agg in window_aggs:
                    feature_name = f"{agg.function}_{agg.column}_{int(time_window.total_seconds())}s"
                    if feature_name not in expected_columns:
                        expected_columns.append(feature_name)

            empty_data = {}
            for col in entity_keys:
                empty_data[col] = pd.Series(dtype="string")
            if self.timestamp_col in expected_columns:
                empty_data[self.timestamp_col] = pd.Series(dtype="datetime64[ns]")
            for col in expected_columns:
                if col not in empty_data:
                    empty_data[col] = pd.Series(dtype="float64")

            empty_pdf = pd.DataFrame(empty_data)
            final_df = self.spark_session.createDataFrame(empty_pdf)
        else:
            # Step 3: Join all windows in pandas (outer merge on entity keys + timestamp)
            if len(windowed_pdfs) == 1:
                final_pdf = windowed_pdfs[0]
            else:
                final_pdf = windowed_pdfs[0]
                join_keys = entity_keys + [self.timestamp_col]
                for pdf in windowed_pdfs[1:]:
                    final_pdf = pd.merge(
                        final_pdf,
                        pdf,
                        on=join_keys,
                        how="outer",
                        suffixes=("", "_dup"),
                    )
                    # Drop duplicate columns from merge
                    final_pdf = final_pdf.loc[
                        :, ~final_pdf.columns.str.endswith("_dup")
                    ]

            # Step 4: Deduplicate in pandas (keep latest timestamp per entity)
            if self.timestamp_col in final_pdf.columns and not final_pdf.empty:
                final_pdf = deduplicate_keep_latest(
                    final_pdf, entity_keys, self.timestamp_col
                )

            # Step 5: Convert to Spark once at the end
            final_df = self.spark_session.createDataFrame(final_pdf)

        return DAGValue(
            data=final_df,
            format=DAGFormat.SPARK,
            metadata={
                "aggregated": True,
                "tiled": True,
                "window_sizes": [
                    int(tw.total_seconds()) for tw in aggs_by_window.keys()
                ],
            },
        )

    def _execute_standard_aggregation(self, input_df: DataFrame) -> DAGValue:
        """Execute standard Spark aggregation (existing logic)."""
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
        column_info: ColumnInfo,
        spark_session: SparkSession,
        inputs: Optional[List[DAGNode]] = None,
        how: str = "inner",
    ):
        super().__init__(name, inputs=inputs or [])
        self.column_info = column_info
        self.spark_session = spark_session
        self.how = how

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_values = self.get_input_values(context)
        for val in input_values:
            val.assert_format(DAGFormat.SPARK)

        # Join all input DataFrames on join_keys
        joined_df = None
        for i, dag_value in enumerate(input_values):
            df = dag_value.data

            # Use original FeatureView name if available
            fv_name = self.inputs[i].name.split(":")[0]
            prefix = fv_name + "__"

            # Skip renaming join keys to preserve join compatibility
            renamed_cols = [
                F.col(c).alias(f"{prefix}{c}")
                if c not in self.column_info.join_keys
                else F.col(c)
                for c in df.columns
            ]
            df = df.select(*renamed_cols)
            if joined_df is None:
                joined_df = df
            else:
                joined_df = joined_df.join(
                    df, on=self.column_info.join_keys, how=self.how
                )

        # If entity_df is provided, join it in last
        entity_df = context.entity_df
        if entity_df is not None:
            entity_df = rename_entity_ts_column(
                spark_session=self.spark_session,
                entity_df=entity_df,
            )
            if joined_df is None:
                raise RuntimeError("No input features available to join with entity_df")

            joined_df = entity_df.join(
                joined_df, on=self.column_info.join_keys, how="left"
            )

        return DAGValue(
            data=joined_df,
            format=DAGFormat.SPARK,
            metadata={"joined_on": self.column_info.join_keys, "join_type": self.how},
        )


class SparkFilterNode(DAGNode):
    def __init__(
        self,
        name: str,
        column_info: ColumnInfo,
        spark_session: SparkSession,
        ttl: Optional[timedelta] = None,
        filter_condition: Optional[str] = None,
        inputs=None,
    ):
        super().__init__(name, inputs=inputs)
        self.column_info = column_info
        self.spark_session = spark_session
        self.ttl = ttl
        self.filter_condition = filter_condition

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.SPARK)
        input_df: DataFrame = input_value.data

        # Get timestamp fields from feature view
        timestamp_column = self.column_info.timestamp_column

        # Optional filter: feature.ts <= entity.event_timestamp
        filtered_df = input_df
        if ENTITY_TS_ALIAS in input_df.columns:
            filtered_df = filtered_df.filter(
                F.col(timestamp_column) <= F.col(ENTITY_TS_ALIAS)
            )

            # Optional TTL filter: feature.ts >= entity.event_timestamp - ttl
            if self.ttl:
                ttl_seconds = int(self.ttl.total_seconds())
                lower_bound = F.col(ENTITY_TS_ALIAS) - F.expr(
                    f"INTERVAL {ttl_seconds} seconds"
                )
                filtered_df = filtered_df.filter(F.col(timestamp_column) >= lower_bound)

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
        column_info: ColumnInfo,
        spark_session: SparkSession,
        inputs=None,
    ):
        super().__init__(name, inputs=inputs)
        self.column_info = column_info
        self.spark_session = spark_session

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.SPARK)
        input_df: DataFrame = input_value.data

        # Dedup based on join keys and event timestamp column
        # Dedup with row_number
        partition_cols = self.column_info.join_keys
        deduped_df = input_df
        if partition_cols:
            ordering = [F.col(self.column_info.timestamp_column).desc()]
            if self.column_info.created_timestamp_column:
                ordering.append(F.col(self.column_info.created_timestamp_column).desc())

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
        inputs=None,
    ):
        super().__init__(name, inputs=inputs)
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
    def __init__(self, name: str, udf: Callable, inputs: List[DAGNode]):
        super().__init__(name, inputs)
        self.udf = udf

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_values = self.get_input_values(context)
        for val in input_values:
            val.assert_format(DAGFormat.SPARK)

        input_dfs: List[DataFrame] = [val.data for val in input_values]

        transformed_df = self.udf(*input_dfs)

        return DAGValue(
            data=transformed_df, format=DAGFormat.SPARK, metadata={"transformed": True}
        )


class SparkValidationNode(DAGNode):
    """
    Spark node for validating feature data against the declared schema.

    Checks that all expected columns are present in the Spark DataFrame,
    validates column types using native Spark types, and checks JSON
    well-formedness for Json columns.
    """

    def __init__(
        self,
        name: str,
        expected_columns: Dict[str, Optional[SparkDataType]],
        json_columns: Optional[Set[str]] = None,
        inputs: Optional[List[DAGNode]] = None,
    ):
        super().__init__(name, inputs=inputs)
        self.expected_columns = expected_columns
        self.json_columns = json_columns or set()

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.SPARK)
        spark_df: DataFrame = input_value.data

        if not self.expected_columns:
            context.node_outputs[self.name] = input_value
            return input_value

        self._validate_schema(spark_df)

        logger.debug("[Validation: %s] Schema validation passed.", self.name)
        context.node_outputs[self.name] = input_value
        return input_value

    def _validate_schema(self, spark_df: DataFrame):
        """Validate the Spark DataFrame against the expected schema.

        Checks for missing columns, type mismatches using native Spark types,
        and JSON well-formedness for declared Json columns.
        """
        actual_columns = set(spark_df.columns)
        expected_names = set(self.expected_columns.keys())

        missing = expected_names - actual_columns
        if missing:
            raise ValueError(
                f"[Validation: {self.name}] Missing expected columns: {missing}. "
                f"Actual columns: {sorted(actual_columns)}"
            )

        # Type validation using native Spark types
        schema = spark_df.schema
        for col_name, expected_type in self.expected_columns.items():
            if expected_type is None:
                continue
            try:
                actual_field = schema[col_name]
            except (KeyError, IndexError):
                continue
            actual_type = actual_field.dataType
            if not _spark_types_compatible(expected_type, actual_type):
                logger.warning(
                    "[Validation: %s] Column '%s' type mismatch: expected %s, got %s",
                    self.name,
                    col_name,
                    expected_type.simpleString(),
                    actual_type.simpleString(),
                )

        # Validate JSON well-formedness for declared Json columns
        if self.json_columns:
            sample_rows = spark_df.limit(1000).collect()
            for col_name in self.json_columns:
                if col_name not in actual_columns:
                    continue

                invalid_count = 0
                first_error = None
                first_error_row = None

                for i, row in enumerate(sample_rows):
                    value = row[col_name]
                    if value is None:
                        continue
                    if not isinstance(value, str):
                        continue
                    try:
                        json.loads(value)
                    except (json.JSONDecodeError, TypeError) as e:
                        invalid_count += 1
                        if first_error is None:
                            first_error = str(e)
                            first_error_row = i

                if invalid_count > 0:
                    raise ValueError(
                        f"[Validation: {self.name}] Column '{col_name}' declared as "
                        f"Json contains {invalid_count} invalid JSON value(s) in "
                        f"sampled rows. First error at row {first_error_row}: "
                        f"{first_error}"
                    )
