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

"""Tests for OnDemandFeatureView input_schema support."""

import copy
from datetime import timedelta

import pandas as pd
import pytest

from feast import Entity, Field
from feast.aggregation import Aggregation, aggregation_specs_to_agg_ops
from feast.on_demand_feature_view import OnDemandFeatureView, on_demand_feature_view
from feast.types import Float64, Int64
from feast.value_type import ValueType

user = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)


def test_decorator_with_input_schema():
    """The @on_demand_feature_view decorator supports input_schema without sources."""

    @on_demand_feature_view(
        input_schema=[
            Field(name="txn_amount", dtype=Float64),
        ],
        schema=[
            Field(name="txn_count", dtype=Int64),
            Field(name="total_txn_amount", dtype=Float64),
            Field(name="avg_txn_amount", dtype=Float64),
        ],
        aggregations=[
            Aggregation(
                column="txn_amount",
                function="count",
                name="txn_count",
                time_window=timedelta(days=30),
            ),
            Aggregation(
                column="txn_amount",
                function="sum",
                name="total_txn_amount",
                time_window=timedelta(days=30),
            ),
            Aggregation(
                column="txn_amount",
                function="mean",
                name="avg_txn_amount",
                time_window=timedelta(days=30),
            ),
        ],
        entities=[user],
    )
    def compute_txn_stats(df: pd.DataFrame) -> pd.DataFrame:
        return df

    assert isinstance(compute_txn_stats, OnDemandFeatureView)
    assert compute_txn_stats.name == "compute_txn_stats"
    assert compute_txn_stats.input_schema == [Field(name="txn_amount", dtype=Float64)]
    assert len(compute_txn_stats.aggregations) == 3
    assert len(compute_txn_stats.features) == 3

    # The internal sentinel RequestSource should be present
    sentinel_name = (
        f"{OnDemandFeatureView._INPUT_SCHEMA_SOURCE_PREFIX}compute_txn_stats"
    )
    assert sentinel_name in compute_txn_stats.source_request_sources

    # sources (user-visible) should be empty
    assert compute_txn_stats.sources == []


def test_aggregation_aliases():
    """Aggregation name and time_window params work correctly."""
    agg = Aggregation(
        column="txn_amount",
        function="sum",
        name="total_txn_amount",
        time_window=timedelta(days=30),
    )
    assert agg.name == "total_txn_amount"
    assert agg.time_window == timedelta(days=30)


def test_input_schema_proto_roundtrip():
    """An ODFV with input_schema survives a to_proto / from_proto round-trip."""

    @on_demand_feature_view(
        input_schema=[
            Field(name="txn_amount", dtype=Float64),
        ],
        schema=[
            Field(name="total_txn_amount", dtype=Float64),
        ],
        aggregations=[
            Aggregation(
                column="txn_amount",
                function="sum",
                name="total_txn_amount",
                time_window=timedelta(days=30),
            ),
        ],
        entities=[user],
    )
    def txn_view(df: pd.DataFrame) -> pd.DataFrame:
        return df

    proto = txn_view.to_proto()
    restored = OnDemandFeatureView.from_proto(proto)

    assert restored.input_schema == txn_view.input_schema
    assert restored.aggregations == txn_view.aggregations
    sentinel_name = f"{OnDemandFeatureView._INPUT_SCHEMA_SOURCE_PREFIX}txn_view"
    assert sentinel_name in restored.source_request_sources


def test_input_schema_copy():
    """__copy__ preserves input_schema and aggregations."""

    @on_demand_feature_view(
        input_schema=[
            Field(name="txn_amount", dtype=Float64),
        ],
        schema=[
            Field(name="total_txn_amount", dtype=Float64),
        ],
        aggregations=[
            Aggregation(column="txn_amount", function="sum", name="total_txn_amount"),
        ],
        entities=[user],
    )
    def copy_view(df: pd.DataFrame) -> pd.DataFrame:
        return df

    cloned = copy.copy(copy_view)
    assert cloned.input_schema == copy_view.input_schema
    assert cloned.aggregations == copy_view.aggregations
    sentinel_name = f"{OnDemandFeatureView._INPUT_SCHEMA_SOURCE_PREFIX}copy_view"
    assert sentinel_name in cloned.source_request_sources


def test_sources_required_without_input_schema():
    """Constructor raises if neither sources nor input_schema is provided."""
    with pytest.raises(
        (ValueError, TypeError),
    ):

        def dummy(df):
            return df

        OnDemandFeatureView(
            name="bad_view",
            schema=[Field(name="out", dtype=Float64)],
            udf=dummy,
        )


def test_aggregation_from_proto_no_time_window_roundtrip():
    """Aggregation.from_proto preserves None time_window (fixes timedelta(0) regression)."""
    agg = Aggregation(column="amount", function="sum", name="total_amount")
    assert agg.time_window is None

    restored = Aggregation.from_proto(agg.to_proto())
    assert restored.time_window is None
    assert restored == agg


def test_aggregation_from_proto_no_time_window_online_serving():
    """Aggregations without time_window don't raise in aggregation_specs_to_agg_ops after roundtrip."""
    agg = Aggregation(column="amount", function="sum", name="total_amount")
    restored = Aggregation.from_proto(agg.to_proto())

    # Should not raise — time_window is None, not timedelta(0)
    result = aggregation_specs_to_agg_ops(
        [restored],
        time_window_unsupported_error_message="Time window aggregation is not supported in online serving",
    )
    assert result == {"total_amount": ("sum", "amount")}
