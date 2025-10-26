from datetime import datetime, timedelta

import pandas as pd
import pytest

from feast.transformation.tiled_transformation import (
    TiledTransformation,
    TileConfiguration,
    tiled_transformation,
)


def test_tile_configuration():
    """Test TileConfiguration creation and validation."""
    config = TileConfiguration(
        tile_size=timedelta(hours=1),
        overlap=timedelta(minutes=5),
        max_tiles_in_memory=5,
        enable_late_data_handling=True,
    )
    
    assert config.tile_size == timedelta(hours=1)
    assert config.overlap == timedelta(minutes=5)
    assert config.max_tiles_in_memory == 5
    assert config.enable_late_data_handling is True


def test_pandas_tiled_transformation_basic():
    """Test basic pandas tiled transformation functionality."""
    
    def simple_transform(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(doubled_value=df['value'] * 2)
    
    config = TileConfiguration(tile_size=timedelta(hours=1))
    
    # Import here to avoid circular imports
    from feast.transformation.pandas_tiled_transformation import PandasTiledTransformation
    transformation = PandasTiledTransformation(
        udf=simple_transform,
        udf_string="lambda df: df.assign(doubled_value=df['value'] * 2)",
        tile_config=config,
        name="test_tiling",
    )
    
    # Create test data with timestamps
    data = pd.DataFrame({
        'timestamp': pd.date_range('2023-01-01 00:00:00', periods=4, freq='30min'),
        'entity_id': ['user1', 'user2', 'user1', 'user2'],
        'value': [10, 20, 30, 40]
    })
    
    result = transformation.transform(data, 'timestamp')
    
    # Check that transformation was applied
    assert 'doubled_value' in result.columns
    assert result['doubled_value'].tolist() == [20, 40, 60, 80]


def test_pandas_tiled_transformation_with_aggregation():
    """Test pandas tiled transformation with aggregation functions."""
    
    def base_transform(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(processed=df['value'] + 1)
    
    def aggregate_func(df: pd.DataFrame) -> pd.DataFrame:
        return df.groupby('entity_id').agg({
            'value': 'sum',
            'processed': 'mean'
        }).reset_index()
    
    config = TileConfiguration(tile_size=timedelta(hours=1))
    
    from feast.transformation.pandas_tiled_transformation import PandasTiledTransformation
    transformation = PandasTiledTransformation(
        udf=base_transform,
        udf_string="lambda df: df.assign(processed=df['value'] + 1)",
        tile_config=config,
        name="test_aggregation",
        aggregation_functions=[aggregate_func],
    )
    
    # Create test data spanning multiple hours
    data = pd.DataFrame({
        'timestamp': pd.date_range('2023-01-01 00:00:00', periods=6, freq='30min'),
        'entity_id': ['user1', 'user2'] * 3,
        'value': [10, 20, 15, 25, 12, 18]
    })
    
    result = transformation.transform(data, 'timestamp')
    
    # Should have aggregated results
    assert 'processed' in result.columns
    assert len(result) <= len(data)  # Should be aggregated


def test_pandas_tiled_transformation_with_chaining():
    """Test pandas tiled transformation with chaining functions."""
    
    def base_transform(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(running_sum=df['value'].cumsum())
    
    def chain_func(prev_df: pd.DataFrame, curr_df: pd.DataFrame) -> pd.DataFrame:
        # Add previous max to current running sum for continuity
        if not prev_df.empty and not curr_df.empty:
            max_prev = prev_df['running_sum'].max()
            curr_df = curr_df.copy()
            curr_df['running_sum'] = curr_df['running_sum'] + max_prev
        return curr_df
    
    config = TileConfiguration(
        tile_size=timedelta(hours=1),
        overlap=timedelta(minutes=10)
    )
    
    from feast.transformation.pandas_tiled_transformation import PandasTiledTransformation
    transformation = PandasTiledTransformation(
        udf=base_transform,
        udf_string="lambda df: df.assign(running_sum=df['value'].cumsum())",
        tile_config=config,
        name="test_chaining",
        chaining_functions=[chain_func],
    )
    
    # Create test data spanning multiple hours
    data = pd.DataFrame({
        'timestamp': pd.date_range('2023-01-01 00:00:00', periods=6, freq='30min'),
        'entity_id': ['user1'] * 6,
        'value': [10, 20, 15, 25, 12, 18]
    })
    
    result = transformation.transform(data, 'timestamp')
    
    # Should have chained running sum
    assert 'running_sum' in result.columns


def test_tiled_transformation_decorator():
    """Test the decorator syntax for tiled transformations."""
    
    @tiled_transformation(
        tile_size=timedelta(hours=1),
        mode="pandas",
        overlap=timedelta(minutes=5),
        aggregation_functions=[
            lambda df: df.groupby('entity_id').agg({'value': 'mean'}).reset_index()
        ]
    )
    def my_tiled_feature(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(scaled_value=df['value'] * 0.5)
    
    # Create test data
    data = pd.DataFrame({
        'timestamp': pd.date_range('2023-01-01 00:00:00', periods=4, freq='30min'),
        'entity_id': ['user1', 'user2', 'user1', 'user2'],
        'value': [100, 200, 150, 250]
    })
    
    result = my_tiled_feature.transform(data, 'timestamp')
    
    # Should have both original transformation and aggregation
    assert 'value' in result.columns


def test_pandas_tiled_transformation_empty_data():
    """Test tiled transformation with empty DataFrame."""
    
    def simple_transform(df: pd.DataFrame) -> pd.DataFrame:
        return df.assign(new_col=1) if not df.empty else df
    
    config = TileConfiguration(tile_size=timedelta(hours=1))
    
    from feast.transformation.pandas_tiled_transformation import PandasTiledTransformation
    transformation = PandasTiledTransformation(
        udf=simple_transform,
        udf_string="lambda df: df.assign(new_col=1) if not df.empty else df",
        tile_config=config,
        name="test_empty",
    )
    
    # Empty DataFrame
    data = pd.DataFrame(columns=['timestamp', 'entity_id', 'value'])
    
    result = transformation.transform(data, 'timestamp')
    
    # Should handle empty data gracefully
    assert result.empty


def test_pandas_tiled_transformation_missing_timestamp():
    """Test error handling when timestamp column is missing."""
    
    def simple_transform(df: pd.DataFrame) -> pd.DataFrame:
        return df
    
    config = TileConfiguration(tile_size=timedelta(hours=1))
    
    from feast.transformation.pandas_tiled_transformation import PandasTiledTransformation
    transformation = PandasTiledTransformation(
        udf=simple_transform,
        udf_string="lambda df: df",
        tile_config=config,
        name="test_missing_timestamp",
    )
    
    # Data without timestamp column
    data = pd.DataFrame({
        'entity_id': ['user1', 'user2'],
        'value': [10, 20]
    })
    
    with pytest.raises(ValueError, match="Timestamp column 'timestamp' not found"):
        transformation.transform(data, 'timestamp')