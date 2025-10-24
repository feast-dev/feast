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
