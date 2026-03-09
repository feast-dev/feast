from datetime import timedelta

import pytest

from feast.aggregation import Aggregation, aggregation_specs_to_agg_ops


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
