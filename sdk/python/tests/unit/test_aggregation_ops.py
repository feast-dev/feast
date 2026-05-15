from datetime import timedelta

import pandas as pd
import pytest

from feast.aggregation import Aggregation, aggregation_specs_to_agg_ops
from feast.aggregation.tiling.base import get_ir_metadata_for_aggregation


class DummyAggregation:
    def __init__(self, *, function: str, column: str, time_window=None, name: str = ""):
        self.function = function
        self.column = column
        self.time_window = time_window
        self.name = name


def test_aggregation_specs_to_agg_ops_success():
    agg_specs = [
        DummyAggregation(function="sum", column="trips"),
        DummyAggregation(function="mean", column="fare"),
    ]

    agg_ops = aggregation_specs_to_agg_ops(
        agg_specs,
        time_window_unsupported_error_message="Time window aggregation is not supported.",
    )

    assert agg_ops == {
        "sum_trips": ("sum", "trips"),
        "mean_fare": ("mean", "fare"),
    }


@pytest.mark.parametrize(
    "error_message",
    [
        "Time window aggregation is not supported in online serving.",
        "Time window aggregation is not supported in the local compute engine.",
    ],
)
def test_aggregation_specs_to_agg_ops_time_window_unsupported(error_message: str):
    agg_specs = [DummyAggregation(function="sum", column="trips", time_window=1)]

    with pytest.raises(ValueError, match=error_message):
        aggregation_specs_to_agg_ops(
            agg_specs,
            time_window_unsupported_error_message=error_message,
        )


def test_aggregation_specs_to_agg_ops_custom_name():
    agg_specs = [
        DummyAggregation(
            function="sum",
            column="seconds_watched",
            name="sum_seconds_watched_per_ad_1d",
        ),
    ]

    agg_ops = aggregation_specs_to_agg_ops(
        agg_specs,
        time_window_unsupported_error_message="Time window aggregation is not supported.",
    )

    assert agg_ops == {
        "sum_seconds_watched_per_ad_1d": ("sum", "seconds_watched"),
    }


def test_aggregation_specs_to_agg_ops_mixed_names():
    agg_specs = [
        DummyAggregation(function="sum", column="trips", name="total_trips"),
        DummyAggregation(function="mean", column="fare"),
    ]

    agg_ops = aggregation_specs_to_agg_ops(
        agg_specs,
        time_window_unsupported_error_message="Time window aggregation is not supported.",
    )

    assert agg_ops == {
        "total_trips": ("sum", "trips"),
        "mean_fare": ("mean", "fare"),
    }


def test_aggregation_round_trip_with_name():
    agg = Aggregation(
        column="seconds_watched",
        function="sum",
        time_window=timedelta(days=1),
        name="sum_seconds_watched_per_ad_1d",
    )
    proto = agg.to_proto()
    assert proto.name == "sum_seconds_watched_per_ad_1d"

    restored = Aggregation.from_proto(proto)
    assert restored.name == "sum_seconds_watched_per_ad_1d"
    assert restored == agg


def test_count_distinct_agg_ops():
    """aggregation_specs_to_agg_ops maps count_distinct to the nunique pandas function."""
    agg_specs = [DummyAggregation(function="count_distinct", column="item_id")]

    agg_ops = aggregation_specs_to_agg_ops(
        agg_specs,
        time_window_unsupported_error_message="no windows",
    )

    assert agg_ops == {"count_distinct_item_id": ("nunique", "item_id")}


def test_count_distinct_result():
    """count_distinct via nunique returns the number of unique values per group."""
    from feast.infra.compute_engines.backends.pandas_backend import PandasBackend

    agg_specs = [DummyAggregation(function="count_distinct", column="item_id")]
    agg_ops = aggregation_specs_to_agg_ops(
        agg_specs,
        time_window_unsupported_error_message="no windows",
    )

    df = pd.DataFrame({"user": ["A", "A", "B"], "item_id": [1, 2, 1]})
    result = PandasBackend().groupby_agg(df, ["user"], agg_ops)
    result = result.set_index("user")

    assert result.loc["A", "count_distinct_item_id"] == 2
    assert result.loc["B", "count_distinct_item_id"] == 1


def test_count_distinct_tiling_raises():
    """get_ir_metadata_for_aggregation raises ValueError for count_distinct."""
    agg = Aggregation(column="item_id", function="count_distinct")
    with pytest.raises(ValueError, match="count_distinct does not support tiling"):
        get_ir_metadata_for_aggregation(agg, "count_distinct_item_id")
