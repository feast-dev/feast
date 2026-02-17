import pytest

from feast.aggregation import aggregation_specs_to_agg_ops


class DummyAggregation:
    def __init__(self, *, function: str, column: str, time_window=None):
        self.function = function
        self.column = column
        self.time_window = time_window


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
