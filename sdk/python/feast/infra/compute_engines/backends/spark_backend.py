from datetime import timedelta
from typing import TYPE_CHECKING, Dict, List, Tuple

import pyarrow as pa
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import asc, col, desc

from feast.infra.compute_engines.backends.base import DataFrameBackend

if TYPE_CHECKING:
    import numpy as np


class SparkBackend(DataFrameBackend):
    """Spark DataFrame backend implementation."""

    def __init__(self):
        if (
            not hasattr(SparkDataFrame, "__name__")
            or SparkDataFrame.__name__ == "SparkDataFrame"
        ):
            raise ImportError(
                "PySpark is not installed. Please install it to use Spark backend."
            )

    def columns(self, df: SparkDataFrame) -> List[str]:
        """Get column names from Spark DataFrame."""
        return df.columns

    def from_arrow(self, table: pa.Table) -> SparkDataFrame:
        """Convert Arrow table to Spark DataFrame."""
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError("No active Spark session found")
        return spark.createDataFrame(table.to_pandas())

    def to_arrow(self, df: SparkDataFrame) -> pa.Table:
        """Convert Spark DataFrame to Arrow table."""
        return pa.Table.from_pandas(df.toPandas())

    def join(
        self, left: SparkDataFrame, right: SparkDataFrame, on: List[str], how: str
    ) -> SparkDataFrame:
        """Join two Spark DataFrames."""
        return left.join(right, on=on, how=how)

    def groupby_agg(
        self,
        df: SparkDataFrame,
        group_keys: List[str],
        agg_ops: Dict[str, Tuple[str, str]],
    ) -> SparkDataFrame:
        """Group and aggregate Spark DataFrame."""

        # Convert agg_ops to Spark aggregation expressions
        agg_exprs = {}
        for alias, (func, column) in agg_ops.items():
            if func == "count":
                agg_exprs[alias] = {"count": column}
            elif func == "sum":
                agg_exprs[alias] = {"sum": column}
            elif func == "avg":
                agg_exprs[alias] = {"avg": column}
            elif func == "min":
                agg_exprs[alias] = {"min": column}
            elif func == "max":
                agg_exprs[alias] = {"max": column}
            else:
                raise ValueError(f"Unsupported aggregation function: {func}")

        # Flatten the expressions for Spark's agg() method
        spark_agg_exprs = {}
        for alias, agg_dict in agg_exprs.items():
            for agg_func, col_name in agg_dict.items():
                spark_agg_exprs[f"{agg_func}({col_name})"] = alias

        result = df.groupBy(*group_keys).agg(spark_agg_exprs)

        # Rename columns to match expected aliases
        for old_name, new_name in spark_agg_exprs.items():
            if old_name != new_name:
                result = result.withColumnRenamed(old_name, new_name)

        return result

    def filter(self, df: SparkDataFrame, expr: str) -> SparkDataFrame:
        """Apply filter expression to Spark DataFrame."""
        return df.filter(expr)

    def to_timedelta_value(self, delta: timedelta) -> str:
        """Convert timedelta to Spark interval string."""
        total_seconds = int(delta.total_seconds())
        if total_seconds < 60:
            return f"INTERVAL {total_seconds} SECONDS"
        elif total_seconds < 3600:
            minutes = total_seconds // 60
            seconds = total_seconds % 60
            if seconds == 0:
                return f"INTERVAL {minutes} MINUTES"
            else:
                return f"INTERVAL '{minutes}:{seconds:02d}' MINUTE TO SECOND"
        else:
            hours = total_seconds // 3600
            remainder = total_seconds % 3600
            minutes = remainder // 60
            seconds = remainder % 60
            return f"INTERVAL '{hours}:{minutes:02d}:{seconds:02d}' HOUR TO SECOND"

    def drop_duplicates(
        self,
        df: SparkDataFrame,
        keys: List[str],
        sort_by: List[str],
        ascending: bool = False,
    ) -> SparkDataFrame:
        """Deduplicate Spark DataFrame by keys, sorted by sort_by columns."""
        from pyspark.sql.functions import row_number
        from pyspark.sql.window import Window

        # Create window spec for deduplication
        window_spec = Window.partitionBy(*keys)

        # Add sort order
        if ascending:
            window_spec = window_spec.orderBy(*[asc(col) for col in sort_by])
        else:
            window_spec = window_spec.orderBy(*[desc(col) for col in sort_by])

        # Add row number and filter to keep only first row
        return (
            df.withColumn("__row_number", row_number().over(window_spec))
            .filter(col("__row_number") == 1)
            .drop("__row_number")
        )

    def rename_columns(
        self, df: SparkDataFrame, columns: Dict[str, str]
    ) -> SparkDataFrame:
        """Rename columns in Spark DataFrame."""
        result = df
        for old_name, new_name in columns.items():
            result = result.withColumnRenamed(old_name, new_name)
        return result

    def get_schema(self, df: SparkDataFrame) -> Dict[str, "np.dtype"]:
        """Get Spark DataFrame schema as column name to numpy dtype mapping."""
        from feast.type_map import spark_schema_to_np_dtypes

        return dict(zip(df.columns, spark_schema_to_np_dtypes(df.dtypes)))

    def get_timestamp_range(self, df: SparkDataFrame, timestamp_column: str) -> tuple:
        """Get min/max of a timestamp column in Spark DataFrame."""
        from pyspark.sql.functions import max as spark_max
        from pyspark.sql.functions import min as spark_min

        result = df.select(
            spark_min(col(timestamp_column)).alias("min_ts"),
            spark_max(col(timestamp_column)).alias("max_ts"),
        ).collect()[0]

        return (result["min_ts"], result["max_ts"])
