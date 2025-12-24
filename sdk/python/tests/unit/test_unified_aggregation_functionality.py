"""
Test unified aggregation functionality using @transformation decorator.

Converted from test_on_demand_feature_view_aggregation.py to demonstrate
aggregation functionality working with the new unified transformation system.
"""

import pyarrow as pa
import pandas as pd
import pytest
from typing import Any, Dict

from feast.aggregation import Aggregation
from feast.utils import _apply_aggregations_to_response
from feast.transformation.base import transformation
from feast.feature_view import FeatureView
from feast.field import Field
from feast.infra.offline_stores.file_source import FileSource
from feast.types import Float32, Int64


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


def test_unified_transformation_with_aggregation_python():
    """Test unified FeatureView with python transformation that performs aggregation."""
    file_source = FileSource(name="my-file-source", path="test.parquet")
    sink_source = FileSource(name="sink-source", path="sink.parquet")
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="trips", dtype=Int64),
            Field(name="revenue", dtype=Float32),
        ],
        source=file_source,
    )

    @transformation(mode="python")
    def aggregation_transform(inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Python transformation that performs aggregation logic."""
        # Create aggregations
        aggs = [
            Aggregation(column="trips", function="sum"),
            Aggregation(column="revenue", function="mean"),
        ]

        # Apply aggregations using the utility function
        result = _apply_aggregations_to_response(
            inputs, aggs, ["driver_id"], "python"
        )
        return result

    # Create unified FeatureView with aggregation transformation
    unified_aggregation_view = FeatureView(
        name="unified_aggregation_view",
        source=[feature_view],
        sink_source=sink_source,
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="sum_trips", dtype=Int64),
            Field(name="mean_revenue", dtype=Float32),
        ],
        feature_transformation=aggregation_transform,
    )

    # Test the transformation directly
    test_data = {
        "driver_id": [1, 1, 2, 2],
        "trips": [10, 20, 15, 25],
        "revenue": [100.0, 200.0, 150.0, 250.0],
    }

    result = unified_aggregation_view.feature_transformation.udf(test_data)

    expected = {
        "driver_id": [1, 2],
        "sum_trips": [30, 40],
        "mean_revenue": [150.0, 200.0],
    }

    assert result == expected


def test_unified_transformation_with_aggregation_pandas():
    """Test unified FeatureView with pandas transformation that performs aggregation."""
    file_source = FileSource(name="my-file-source", path="test.parquet")
    sink_source = FileSource(name="sink-source", path="sink.parquet")
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="trips", dtype=Int64),
            Field(name="revenue", dtype=Float32),
        ],
        source=file_source,
    )

    @transformation(mode="pandas")
    def pandas_aggregation_transform(inputs: pd.DataFrame) -> pd.DataFrame:
        """Pandas transformation that performs aggregation using groupby."""
        # Perform aggregation using pandas groupby
        result = inputs.groupby("driver_id").agg({
            "trips": "sum",
            "revenue": "mean"
        }).reset_index()

        # Rename columns to match expected output
        result = result.rename(columns={
            "trips": "sum_trips",
            "revenue": "mean_revenue"
        })

        return result

    # Create unified FeatureView with pandas aggregation transformation
    unified_pandas_aggregation_view = FeatureView(
        name="unified_pandas_aggregation_view",
        source=[feature_view],
        sink_source=sink_source,
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="sum_trips", dtype=Int64),
            Field(name="mean_revenue", dtype=Float32),
        ],
        feature_transformation=pandas_aggregation_transform,
    )

    # Test the transformation directly
    test_data = pd.DataFrame({
        "driver_id": [1, 1, 2, 2],
        "trips": [10, 20, 15, 25],
        "revenue": [100.0, 200.0, 150.0, 250.0],
    })

    result = unified_pandas_aggregation_view.feature_transformation.udf(test_data)

    # Convert to dict for comparison
    result_dict = result.to_dict(orient="list")

    expected = {
        "driver_id": [1, 2],
        "sum_trips": [30, 40],
        "mean_revenue": [150.0, 200.0],
    }

    assert result_dict == expected


def test_unified_transformation_with_custom_aggregation():
    """Test unified FeatureView with custom aggregation logic."""
    file_source = FileSource(name="my-file-source", path="test.parquet")
    sink_source = FileSource(name="sink-source", path="sink.parquet")
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="trips", dtype=Int64),
            Field(name="revenue", dtype=Float32),
        ],
        source=file_source,
    )

    @transformation(mode="python")
    def custom_aggregation_transform(inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Custom python transformation with manual aggregation logic."""
        # Manual aggregation without using the utility function
        driver_data = {}

        for i, driver_id in enumerate(inputs["driver_id"]):
            if driver_id not in driver_data:
                driver_data[driver_id] = {"trips": [], "revenue": []}

            driver_data[driver_id]["trips"].append(inputs["trips"][i])
            driver_data[driver_id]["revenue"].append(inputs["revenue"][i])

        # Calculate aggregations
        result = {
            "driver_id": [],
            "sum_trips": [],
            "mean_revenue": [],
            "max_trips": [],  # Additional custom aggregation
        }

        for driver_id, data in driver_data.items():
            result["driver_id"].append(driver_id)
            result["sum_trips"].append(sum(data["trips"]))
            result["mean_revenue"].append(sum(data["revenue"]) / len(data["revenue"]))
            result["max_trips"].append(max(data["trips"]))

        return result

    # Create unified FeatureView with custom aggregation
    unified_custom_aggregation_view = FeatureView(
        name="unified_custom_aggregation_view",
        source=[feature_view],
        sink_source=sink_source,
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="sum_trips", dtype=Int64),
            Field(name="mean_revenue", dtype=Float32),
            Field(name="max_trips", dtype=Int64),
        ],
        feature_transformation=custom_aggregation_transform,
    )

    # Test the transformation
    test_data = {
        "driver_id": [1, 1, 2, 2],
        "trips": [10, 20, 15, 25],
        "revenue": [100.0, 200.0, 150.0, 250.0],
    }

    result = unified_custom_aggregation_view.feature_transformation.udf(test_data)

    expected = {
        "driver_id": [1, 2],
        "sum_trips": [30, 40],
        "mean_revenue": [150.0, 200.0],
        "max_trips": [20, 25],
    }

    assert result == expected


def test_unified_transformation_aggregation_with_write():
    """Test unified FeatureView aggregation with write_to_online_store (batch_on_write)."""
    file_source = FileSource(name="my-file-source", path="test.parquet")
    sink_source = FileSource(name="sink-source", path="sink.parquet")
    feature_view = FeatureView(
        name="my-feature-view",
        entities=[],
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="trips", dtype=Int64),
        ],
        source=file_source,
    )

    @transformation(mode="python")
    def write_aggregation_transform(inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Aggregation transformation for online writes."""
        aggs = [Aggregation(column="trips", function="sum")]
        return _apply_aggregations_to_response(inputs, aggs, ["driver_id"], "python")

    # Create unified FeatureView with aggregation for online writes
    unified_write_aggregation_view = FeatureView(
        name="unified_write_aggregation_view",
        source=[feature_view],
        sink_source=sink_source,
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="sum_trips", dtype=Int64),
        ],
        feature_transformation=write_aggregation_transform,
    )

    # Verify online setting
    assert unified_write_aggregation_view.online == True

    # Test the transformation
    test_data = {
        "driver_id": [1, 1, 2, 2],
        "trips": [10, 20, 15, 25],
    }

    result = unified_write_aggregation_view.feature_transformation.udf(test_data)

    expected = {
        "driver_id": [1, 2],
        "sum_trips": [30, 40],
    }

    assert result == expected