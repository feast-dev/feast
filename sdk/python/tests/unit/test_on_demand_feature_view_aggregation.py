# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for OnDemandFeatureView aggregations in online serving."""

import pyarrow as pa

from feast.aggregation import Aggregation
from feast.field import Field
from feast.on_demand_feature_view import OnDemandFeatureView, on_demand_feature_view
from feast.types import Float32, Int64
from feast.utils import _apply_aggregations_to_response


def test_aggregation_python_mode():
    """Test aggregations in Python mode (dict format)."""
    data = {
        "driver_id": [1, 1, 2, 2],
        "trips": [10, 20, 15, 25],
    }
    aggs = [Aggregation(column="trips", function="sum")]

    result = _apply_aggregations_to_response(data, aggs, ["driver_id"], "python")

    assert result == {"driver_id": [1, 2], "sum_trips": [30, 40]}


def test_aggregation_pandas_mode():
    """Test aggregations in Pandas mode (Arrow table format)."""
    table = pa.table(
        {
            "driver_id": [1, 1, 2, 2],
            "trips": [10, 20, 15, 25],
        }
    )
    aggs = [Aggregation(column="trips", function="sum")]

    result = _apply_aggregations_to_response(table, aggs, ["driver_id"], "pandas")

    assert isinstance(result, pa.Table)
    result_df = result.to_pandas()
    assert list(result_df["driver_id"]) == [1, 2]
    assert list(result_df["sum_trips"]) == [30, 40]


def test_multiple_aggregations():
    """Test multiple aggregation functions."""
    data = {
        "driver_id": [1, 1, 2, 2],
        "trips": [10, 20, 15, 25],
        "revenue": [100.0, 200.0, 150.0, 250.0],
    }
    aggs = [
        Aggregation(column="trips", function="sum"),
        Aggregation(column="revenue", function="mean"),
    ]

    result = _apply_aggregations_to_response(data, aggs, ["driver_id"], "python")

    assert result["driver_id"] == [1, 2]
    assert result["sum_trips"] == [30, 40]
    assert result["mean_revenue"] == [150.0, 200.0]


def test_no_aggregations_returns_original():
    """Test that no aggregations returns original data."""
    data = {"driver_id": [1, 2], "trips": [10, 20]}

    result = _apply_aggregations_to_response(data, [], ["driver_id"], "python")

    assert result == data


def test_empty_data_returns_empty():
    """Test that empty data returns empty result."""
    data = {"driver_id": [], "trips": []}
    aggs = [Aggregation(column="trips", function="sum")]

    result = _apply_aggregations_to_response(data, aggs, ["driver_id"], "python")

    assert result == data


def test_aggregation_without_time_window_survives_proto_roundtrip():
    """An Aggregation with no time window keeps None through to_proto/from_proto."""
    agg = Aggregation(column="trips", function="sum")
    assert agg.time_window is None
    assert agg.slide_interval is None

    restored = Aggregation.from_proto(agg.to_proto())

    assert restored.time_window is None
    assert restored.slide_interval is None
    assert restored == agg


def test_odfv_aggregation_still_serves_online_after_proto_roundtrip():
    """A registry round-trip must not turn an unset window into a zero window."""

    @on_demand_feature_view(
        sources=[],
        input_schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="trips", dtype=Int64),
        ],
        schema=[Field(name="sum_trips", dtype=Float32)],
        aggregations=[Aggregation(column="trips", function="sum")],
        mode="python",
    )
    def agg_view(inputs):
        return {"sum_trips": inputs["sum_trips"]}

    restored = OnDemandFeatureView.from_proto(agg_view.to_proto())

    data = {"driver_id": [1, 1, 2, 2], "trips": [10, 20, 15, 25]}
    result = _apply_aggregations_to_response(
        data, restored.aggregations, ["driver_id"], "python"
    )

    assert result == {"driver_id": [1, 2], "sum_trips": [30, 40]}
