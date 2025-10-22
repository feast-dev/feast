from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from feast.transformation.tiled_transformation import TiledTransformation, TileConfiguration


class PandasTiledTransformation(TiledTransformation):
    """
    Pandas-specific implementation of tiled transformations for streaming feature engineering.
    
    This class implements efficient temporal windowing and aggregation for Pandas DataFrames,
    following Chronon's tiled architecture patterns.
    """
    
    def __init__(
        self,
        udf: Callable[[pd.DataFrame], pd.DataFrame],
        udf_string: str,
        tile_config: TileConfiguration,
        sources: Optional[List[Any]] = None,
        schema: Optional[List[Any]] = None,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        aggregation_functions: Optional[List[Callable[[pd.DataFrame], pd.DataFrame]]] = None,
        chaining_functions: Optional[List[Callable[[pd.DataFrame, pd.DataFrame], pd.DataFrame]]] = None,
    ):
        super().__init__(
            udf=udf,
            udf_string=udf_string,
            tile_config=tile_config,
            sources=sources,
            schema=schema,
            name=name,
            tags=tags,
            description=description,
            owner=owner,
            aggregation_functions=aggregation_functions,
            chaining_functions=chaining_functions,
        )
    
    def transform(self, data: pd.DataFrame, timestamp_column: str = "timestamp") -> pd.DataFrame:
        """
        Apply tiled transformation to a Pandas DataFrame.
        
        Args:
            data: Input DataFrame with timestamp column
            timestamp_column: Name of the timestamp column for tiling
            
        Returns:
            Transformed DataFrame with tiled processing applied
        """
        if timestamp_column not in data.columns:
            raise ValueError(f"Timestamp column '{timestamp_column}' not found in DataFrame")
        
        # Ensure timestamp column is datetime
        if not pd.api.types.is_datetime64_any_dtype(data[timestamp_column]):
            data[timestamp_column] = pd.to_datetime(data[timestamp_column])
        
        return self._apply_tiled_processing(data, timestamp_column)
    
    def _partition_into_tiles(self, data: pd.DataFrame, timestamp_column: str) -> Dict[str, pd.DataFrame]:
        """
        Partition DataFrame into time-based tiles.
        
        Args:
            data: Input DataFrame
            timestamp_column: Column to use for time-based partitioning
            
        Returns:
            Dictionary mapping tile keys to DataFrame chunks
        """
        tiles = {}
        
        if data.empty:
            return tiles
        
        # Sort by timestamp to ensure proper tile processing order
        data_sorted = data.sort_values(timestamp_column)
        
        # Find time range
        min_time = data_sorted[timestamp_column].min()
        max_time = data_sorted[timestamp_column].max()
        
        # Create tile boundaries
        tile_start = min_time
        tile_index = 0
        
        while tile_start <= max_time:
            tile_end = tile_start + self.tile_config.tile_size
            
            # Apply overlap if configured
            if self.tile_config.overlap.total_seconds() > 0 and tile_index > 0:
                tile_start_adjusted = tile_start - self.tile_config.overlap
            else:
                tile_start_adjusted = tile_start
            
            # Filter data for this tile
            tile_mask = (
                (data_sorted[timestamp_column] >= tile_start_adjusted) &
                (data_sorted[timestamp_column] < tile_end)
            )
            
            tile_data = data_sorted[tile_mask]
            
            if not tile_data.empty:
                tile_key = f"tile_{tile_index}_{tile_start.isoformat()}"
                tiles[tile_key] = tile_data.copy()
            
            # Move to next tile
            tile_start = tile_end
            tile_index += 1
        
        return tiles
    
    def _process_single_tile(self, tile_data: pd.DataFrame, tile_key: str) -> pd.DataFrame:
        """
        Process a single tile of DataFrame data.
        
        Args:
            tile_data: DataFrame for this tile
            tile_key: Unique identifier for this tile
            
        Returns:
            Processed DataFrame
        """
        # Apply the main UDF to the tile
        result = self.udf(tile_data)
        
        # Apply any tile-specific aggregations
        for agg_func in self.aggregation_functions:
            result = agg_func(result)
        
        # Cache management
        if len(self._tile_cache) >= self.tile_config.max_tiles_in_memory:
            # Remove oldest tile
            if self._tile_timestamps:
                oldest_key = self._tile_timestamps.pop(0)
                self._tile_cache.pop(oldest_key, None)
        
        self._tile_cache[tile_key] = result
        self._tile_timestamps.append(tile_key)
        
        return result
    
    def _chain_tile_results(self, processed_tiles: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Chain results across tiles for derived features.
        
        Args:
            processed_tiles: List of processed tile DataFrames
            
        Returns:
            Chained result DataFrame
        """
        if not processed_tiles:
            return pd.DataFrame()
        
        result = processed_tiles[0]
        
        for chain_func in self.chaining_functions:
            for i in range(1, len(processed_tiles)):
                result = chain_func(result, processed_tiles[i])
        
        return result
    
    def _combine_tiles(self, processed_tiles: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Combine processed tiles into final result DataFrame.
        
        Args:
            processed_tiles: List of processed tile DataFrames
            
        Returns:
            Combined result DataFrame
        """
        if not processed_tiles:
            return pd.DataFrame()
        
        # Concatenate all tiles
        combined = pd.concat(processed_tiles, ignore_index=True)
        
        # Remove duplicates if overlap was used (keep most recent)
        if self.tile_config.overlap.total_seconds() > 0 and 'timestamp' in combined.columns:
            combined = combined.drop_duplicates(
                subset=['timestamp'] + [col for col in combined.columns if col.startswith('entity_')],
                keep='last'
            )
        
        return combined
    
    def infer_features(self, random_input: Dict[str, List[Any]], *args, **kwargs) -> List[Any]:
        """
        Infer feature schema from tiled transformation.
        
        Args:
            random_input: Sample input data for schema inference
            
        Returns:
            Inferred feature list
        """
        # Create sample DataFrame
        df = pd.DataFrame.from_dict(random_input)
        
        # Add timestamp if not present
        if 'timestamp' not in df.columns:
            df['timestamp'] = pd.Timestamp.now()
        
        # Apply transformation to sample
        sample_tile = self._process_single_tile(df, "sample_tile")
        
        # Use pandas transformation logic for feature inference
        from feast.transformation.pandas_transformation import PandasTransformation
        temp_pandas_transform = PandasTransformation(
            udf=lambda x: sample_tile,
            udf_string="lambda x: sample_tile"
        )
        
        return temp_pandas_transform.infer_features(random_input, *args, **kwargs)


def pandas_tiled_transformation(
    tile_size: timedelta,
    overlap: Optional[timedelta] = None,
    max_tiles_in_memory: int = 10,
    enable_late_data_handling: bool = True,
    aggregation_functions: Optional[List[Callable[[pd.DataFrame], pd.DataFrame]]] = None,
    chaining_functions: Optional[List[Callable[[pd.DataFrame, pd.DataFrame], pd.DataFrame]]] = None,
    name: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    description: str = "",
    owner: str = "",
):
    """
    Decorator for creating Pandas-specific tiled transformations.
    
    Example:
        @pandas_tiled_transformation(
            tile_size=timedelta(hours=1),
            overlap=timedelta(minutes=5),
            aggregation_functions=[
                lambda df: df.groupby('entity_id').agg({
                    'value': ['sum', 'mean', 'count']
                }).reset_index()
            ]
        )
        def hourly_aggregated_features(df: pd.DataFrame) -> pd.DataFrame:
            # Apply transformations within each 1-hour tile
            return df.assign(
                rolling_avg=df['value'].rolling(window=10).mean(),
                cumsum=df['value'].cumsum()
            )
    """
    def decorator(user_function):
        import dill
        
        udf_string = dill.source.getsource(user_function)
        tile_config = TileConfiguration(
            tile_size=tile_size,
            overlap=overlap,
            max_tiles_in_memory=max_tiles_in_memory,
            enable_late_data_handling=enable_late_data_handling,
        )
        
        return PandasTiledTransformation(
            udf=user_function,
            udf_string=udf_string,
            tile_config=tile_config,
            name=name or user_function.__name__,
            tags=tags,
            description=description,
            owner=owner,
            aggregation_functions=aggregation_functions,
            chaining_functions=chaining_functions,
        )
    
    return decorator