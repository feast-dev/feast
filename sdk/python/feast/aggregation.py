from datetime import timedelta
from typing import List, Optional

from google.protobuf.duration_pb2 import Duration
from pyspark.sql import functions

from feast.protos.feast.core.Aggregation_pb2 import Aggregation as AggregationProto

SUPPORTED_AGGREGATIONS = {"count", "sum", "min", "max", "mean"}


class Aggregation:
    """
    NOTE: Feast-handled aggregations are not yet supported. This class provides a way to register user-defined aggregations.

    Attributes:
        column: str  # Column name of the feature we are aggregating.
        function: str  # Provided built in aggregations sum, max, min, count mean
        time_window: timedelta  # The time window for this aggregation.
        slide_interval: timedelta # The sliding window for these aggregations
    """

    column: str
    function: str
    time_window: Optional[timedelta]
    slide_interval: Optional[timedelta]

    def __init__(
        self,
        column: Optional[str] = "",
        function: Optional[str] = "",
        time_window: Optional[timedelta] = None,
        slide_interval: Optional[timedelta] = None,
    ):
        self.column = column or ""
        self.function = function or ""
        if self.function not in SUPPORTED_AGGREGATIONS:
            raise ValueError(f"aggregation is not supported {self.function}")
        self.time_window = time_window
        if not slide_interval:
            self.slide_interval = self.time_window
        else:
            self.slide_interval = slide_interval

    def to_proto(self) -> AggregationProto:
        window_duration = None
        if self.time_window is not None:
            window_duration = Duration()
            window_duration.FromTimedelta(self.time_window)

        slide_interval_duration = None
        if self.slide_interval is not None:
            slide_interval_duration = Duration()
            slide_interval_duration.FromTimedelta(self.slide_interval)

        return AggregationProto(
            column=self.column,
            function=self.function,
            time_window=window_duration,
            slide_interval=slide_interval_duration,
        )

    @classmethod
    def from_proto(cls, agg_proto: AggregationProto):
        time_window = (
            timedelta(days=0)
            if agg_proto.time_window.ToNanoseconds() == 0
            else agg_proto.time_window.ToTimedelta()
        )

        slide_interval = (
            timedelta(days=0)
            if agg_proto.slide_interval.ToNanoseconds() == 0
            else agg_proto.slide_interval.ToTimedelta()
        )
        aggregation = cls(
            column=agg_proto.column,
            function=agg_proto.function,
            time_window=time_window,
            slide_interval=slide_interval,
        )
        return aggregation

    def __eq__(self, other):
        if not isinstance(other, Aggregation):
            raise TypeError("Comparisons should only involve Aggregations.")

        if (
            self.column != other.column
            or self.function != other.function
            or self.time_window != other.time_window
            or self.slide_interval != other.slide_interval
        ):
            return False

        return True


def _simple_full_aggregation_transform(spark_transform, col, alias_col=None):
    if alias_col:
        return spark_transform(col).alias(alias_col)
    else:
        return spark_transform(col)


def get_aggregation_transform(aggregation_function):
    plan = AGGREGATION_PLANS.get(aggregation_function, None)

    if plan is None:
        raise ValueError(f"Unsupported aggregation function {aggregation_function}")

    return plan


AGGREGATION_PLANS = {
    "count": functions.count,
    "min": functions.min,
    "max": functions.max,
    "mean": functions.mean,
    "sum": functions.sum,
}


def construct_aggregation_plan_df(
    df,
    join_keys: List[str],
    timestamp_field,
    aggregation_functions: List[Aggregation],
    sliding_interval: timedelta,
    time_window: timedelta,
    watermark: timedelta,
):
    """
   Assumes that time window configuration(sliding_interval, time_window, watermark) is the same for all aggregation functions that
   are passed in.
   """
    group_by_cols = [functions.col(join_key) for join_key in join_keys]
    sliding_interval = f"{sliding_interval.total_seconds()} seconds"
    watermark_interval = f"{watermark.total_seconds()} seconds"
    time_interval = f"{time_window.total_seconds()} seconds"
    window_spec = functions.window(timestamp_field, time_interval, sliding_interval)
    group_by_cols = [window_spec] + group_by_cols

    # Based on aggregation functions construct partial aggregations on certain columns.
    # TODO: Fill this logic out => based on how I design aggregation functions
    # fill in aggregations using aggregation functions
    aggregations = []
    for aggregation_function in aggregation_functions:
        transform = get_aggregation_transform(aggregation_function.function)
        alias_col = f"{aggregation_function.column}_{aggregation_function.function}_{aggregation_function.time_window.seconds}s"
        aggregations.append(
            _simple_full_aggregation_transform(
                transform, aggregation_function.column, alias_col=alias_col
            )
        )
    output_df = (
        df.withWatermark(timestamp_field, watermark_interval)
        .groupBy(*group_by_cols)
        .agg(*aggregations)
    )

    # transform input aggregation columns into output columns in output_df
    return output_df
