from datetime import timedelta
from typing import Optional

from google.protobuf.duration_pb2 import Duration
from typeguard import typechecked

from feast.protos.feast.core.Aggregation_pb2 import Aggregation as AggregationProto


@typechecked
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
