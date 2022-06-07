from datetime import timedelta
from typing import Optional

from google.protobuf.duration_pb2 import Duration

from feast.protos.feast.core.Aggregation_pb2 import Aggregation as AggregationProto


class Aggregation:
    """
    NOTE: Feast-handled aggregations are not yet supported. This class provides a way to register user-defined aggregations.

    Attributes:
        column: str  # Column name of the feature we are aggregating.
        function: str  # Provided built in aggregations sum, max, min, count mean
        time_window: timedelta  # The time window for this aggregation.
        sliding_window: timedelta # The sliding window for these aggregations
    """

    column: str
    function: str
    time_window: Optional[timedelta]
    sliding_window: timedelta

    def __init__(
        self,
        column: Optional[str] = "",
        function: Optional[str] = "",
        time_window: Optional[timedelta] = None,
        sliding_window: Optional[timedelta] = None,
    ):
        self.column = column or ""
        self.function = function or ""
        self.time_window = time_window
        if not sliding_window:
            self.sliding_window = self.time_window
        else:
            self.sliding_window = sliding_window

    def to_proto(self) -> AggregationProto:
        window_duration = None
        if self.time_window is not None:
            window_duration = Duration()
            window_duration.FromTimedelta(self.time_window)

        sliding_window_duration = None
        if self.sliding_window is not None:
            sliding_window_duration = Duration()
            sliding_window_duration.FromTimedelta(self.sliding_window)

        return AggregationProto(
            column=self.column,
            function=self.function,
            time_window=window_duration,
            sliding_window=sliding_window_duration,
        )

    @classmethod
    def from_proto(cls, agg_proto: AggregationProto):
        time_window = (
            timedelta(days=0)
            if agg_proto.time_window.ToNanoseconds() == 0
            else agg_proto.time_window.ToTimedelta()
        )

        sliding_window = (
            timedelta(days=0)
            if agg_proto.sliding_window.ToNanoseconds() == 0
            else agg_proto.sliding_window.ToTimedelta()
        )
        aggregation = cls(
            column=agg_proto.column,
            function=agg_proto.function,
            time_window=time_window,
            sliding_window=sliding_window,
        )
        return aggregation

    def __eq__(self, other):
        if not isinstance(other, Aggregation):
            raise TypeError("Comparisons should only involve Aggregations.")

        if (
            self.column != other.column
            or self.function != other.function
            or self.time_window != other.time_window
            or self.sliding_window != other.sliding_window
        ):
            return False

        return True
