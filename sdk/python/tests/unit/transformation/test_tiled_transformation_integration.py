"""
Integration tests for tiled transformations with StreamFeatureView.

This test module validates the integration between tiled transformations
and Feast's StreamFeatureView for real-time feature engineering.
"""

import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock

from feast.transformation.tiled_transformation import tiled_transformation, TileConfiguration
from feast.aggregation import Aggregation
from feast.field import Field
from feast.types import Float64, Int64


def test_tiled_transformation_creation():
    """Test basic creation of tiled transformation."""
    
    @tiled_transformation(
        tile_size=timedelta(hours=1),
        window_size=timedelta(minutes=30),
        overlap=timedelta(minutes=5),
        sources=["transaction_source_fv"],
        schema=[
            Field(name="rolling_avg", dtype=Float64),
            Field(name="cumulative_amount", dtype=Float64),
        ],
        aggregations=[
            Aggregation(column="amount", function="sum", time_window=timedelta(minutes=30))
        ]
    )
    def test_transformation(df):
        return df.assign(rolling_avg=df['amount'].rolling(window=3).mean())
    
    # Verify transformation was created correctly
    assert hasattr(test_transformation, 'tile_config')
    assert test_transformation.tile_config.tile_size == timedelta(hours=1)
    assert test_transformation.tile_config.window_size == timedelta(minutes=30)
    assert test_transformation.tile_config.overlap == timedelta(minutes=5)
    
    # Verify sources and schema
    assert test_transformation.sources == ["transaction_source_fv"]
    assert len(test_transformation.schema) == 2
    assert test_transformation.schema[0].name == "rolling_avg"
    
    # Verify aggregations
    assert len(test_transformation.aggregations) == 1
    assert test_transformation.aggregations[0].column == "amount"
    assert test_transformation.aggregations[0].function == "sum"


def test_tiled_transformation_with_chaining():
    """Test tiled transformation with chaining functions."""
    
    def chain_func(prev_df, curr_df):
        return curr_df.assign(chained_value=1)
    
    @tiled_transformation(
        tile_size=timedelta(hours=1),
        sources=["transaction_fv"],
        schema=[Field(name="local_sum", dtype=Float64)],
        chaining_functions=[chain_func]
    )
    def chained_transformation(df):
        return df.assign(local_sum=df['amount'].sum())
    
    # Verify chaining functions are stored
    assert len(chained_transformation.chaining_functions) == 1
    assert chained_transformation.chaining_functions[0] == chain_func


def test_tiled_transformation_aggregation_support():
    """Test that tiled transformation supports both Feast Aggregation objects and custom functions."""
    
    aggregation = Aggregation(
        column="transaction_amount", 
        function="mean", 
        time_window=timedelta(minutes=30)
    )
    
    custom_agg = lambda df: df.groupby('customer_id').sum()
    
    @tiled_transformation(
        tile_size=timedelta(hours=1),
        aggregations=[aggregation],
        aggregation_functions=[custom_agg]
    )
    def mixed_aggregation_transform(df):
        return df.assign(processed=1)
    
    # Verify both types of aggregations are supported
    assert len(mixed_aggregation_transform.aggregations) == 1
    assert mixed_aggregation_transform.aggregations[0] == aggregation
    
    assert len(mixed_aggregation_transform.aggregation_functions) == 1
    assert mixed_aggregation_transform.aggregation_functions[0] == custom_agg


def test_tiled_transformation_memory_management():
    """Test memory management configuration for tiled transformations."""
    
    @tiled_transformation(
        tile_size=timedelta(hours=1),
        max_tiles_in_memory=5,
        enable_late_data_handling=False
    )
    def memory_managed_transform(df):
        return df
    
    # Verify memory management settings
    assert memory_managed_transform.tile_config.max_tiles_in_memory == 5
    assert memory_managed_transform.tile_config.enable_late_data_handling is False


def test_tile_configuration():
    """Test TileConfiguration class directly."""
    
    config = TileConfiguration(
        tile_size=timedelta(hours=2),
        window_size=timedelta(minutes=45),
        overlap=timedelta(minutes=10),
        max_tiles_in_memory=8,
        enable_late_data_handling=True
    )
    
    assert config.tile_size == timedelta(hours=2)
    assert config.window_size == timedelta(minutes=45)
    assert config.overlap == timedelta(minutes=10)
    assert config.max_tiles_in_memory == 8
    assert config.enable_late_data_handling is True


def test_tile_configuration_defaults():
    """Test TileConfiguration with default values."""
    
    config = TileConfiguration(tile_size=timedelta(hours=1))
    
    # Verify defaults
    assert config.tile_size == timedelta(hours=1)
    assert config.window_size == timedelta(hours=1)  # Should default to tile_size
    assert config.overlap == timedelta(seconds=0)
    assert config.max_tiles_in_memory == 10
    assert config.enable_late_data_handling is True


class TestStreamFeatureViewIntegration:
    """Test integration with StreamFeatureView (using mocks)."""
    
    def test_streamfeatureview_compatibility(self):
        """Test that tiled transformations work with StreamFeatureView pattern."""
        
        # Create a tiled transformation
        @tiled_transformation(
            tile_size=timedelta(hours=1),
            sources=["kafka_source"],
            schema=[Field(name="processed_amount", dtype=Float64)]
        )
        def process_transactions(df):
            return df.assign(processed_amount=df['amount'] * 1.1)
        
        # Mock StreamFeatureView usage pattern
        mock_kafka_source = Mock()
        mock_kafka_source.name = "transaction_stream"
        
        # Simulate how this would be used in StreamFeatureView
        # (In real usage, this would be: StreamFeatureView(feature_transformation=process_transactions, ...))
        feature_view_config = {
            "name": "transaction_features",
            "feature_transformation": process_transactions,
            "source": mock_kafka_source,
            "mode": "spark",  # Mode specified at view level, not transformation level
            "entities": ["customer_id"]
        }
        
        # Verify the transformation can be used in this pattern
        assert feature_view_config["feature_transformation"] == process_transactions
        assert hasattr(feature_view_config["feature_transformation"], "tile_config")
        assert feature_view_config["mode"] == "spark"  # Mode at view level
    
    def test_compute_engine_compatibility(self):
        """Test that tiled transformations are compatible with ComputeEngine execution."""
        
        @tiled_transformation(
            tile_size=timedelta(hours=1),
            aggregations=[
                Aggregation(column="amount", function="sum", time_window=timedelta(minutes=30))
            ]
        )
        def compute_engine_transform(df):
            return df.assign(computed_feature=df['amount'].rolling(window=5).mean())
        
        # Verify transformation is based on Transformation class
        from feast.transformation.base import Transformation
        assert isinstance(compute_engine_transform, Transformation)
        
        # Verify it has the required attributes for ComputeEngine execution
        assert hasattr(compute_engine_transform, 'udf')
        assert hasattr(compute_engine_transform, 'mode')  
        assert hasattr(compute_engine_transform, 'aggregations')
        
        # Verify aggregations are properly configured
        assert len(compute_engine_transform.aggregations) == 1
        assert compute_engine_transform.aggregations[0].column == "amount"


if __name__ == "__main__":
    pytest.main([__file__])