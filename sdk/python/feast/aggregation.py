import abc
from datetime import timedelta
from typing import List, Union

from feast.protos.feast.core.Aggregation_pb2 import (
    Aggregation as AggregationProto,
)

class Aggregation(abc.ABC):
    """
    NOTE: Feast-handled aggregations are not yet supported. This class provides a way to register user-defined aggregations.

    Attributes:
        column: str  # Column name of the feature we are aggregating.
        function: str  # Provided built in aggregations sum, max, min, count mean
        time_windows: Union[timedelta, List[timedelta]]  # The time windows for aggregations.
    """

    column: str
    function: str
    time_windows: List[timedelta]

    def __init__(self, column: str, function: str, time_windows: Union[timedelta, List[timedelta]]):
        self.column = column
        self.function = function
        _time_windows = [time_windows] if not isinstance(time_windows, list) else time_windows
        self.time_windows = _time_windows

    def to_proto(self) -> AggregationProto:
        return AggregationProto(
            column=self.column, function=self.function, time_windows=self.time_windows,
        )

    @classmethod
    def from_proto(cls, agg_proto: AggregationProto):
        aggregation = cls(
            column=agg_proto.column,
            function=agg_proto.function,
            time_windows=list(agg_proto.time_windows),
        )
        return aggregation

    def __eq__(self, other):
        if not isinstance(other, Aggregation):
            raise TypeError("Comparisons should only involve Aggregations.")

        if (
            self.column != other.column
            or self.function != other.function
            or sorted(self.time_windows) != sorted(other.time_windows)
        ):
            return False

        return True