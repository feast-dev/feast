from datetime import timedelta
from typing import Any, Callable, Dict, List, Optional, Union, TYPE_CHECKING

from feast.transformation.base import Transformation
from feast.transformation.mode import TransformationMode

if TYPE_CHECKING:
    from feast.feature_view import FeatureView
    from feast.data_source import DataSource
    from feast.field import Field
    from feast.aggregation import Aggregation


class TileConfiguration:
    """
    Configuration for tiling in streaming transformations.
    
    Attributes:
        tile_size: The size of each time tile (e.g., 1 hour)
        window_size: The window size for aggregations within tiles
        overlap: Optional overlap between tiles for continuity
        max_tiles_in_memory: Maximum number of tiles to keep in memory
        enable_late_data_handling: Whether to handle late-arriving data
    """
    
    def __init__(
        self,
        tile_size: timedelta,
        window_size: Optional[timedelta] = None,
        overlap: Optional[timedelta] = None,
        max_tiles_in_memory: int = 10,
        enable_late_data_handling: bool = True,
    ):
        self.tile_size = tile_size
        self.window_size = window_size or tile_size  # Default window_size to tile_size
        self.overlap = overlap or timedelta(seconds=0)
        self.max_tiles_in_memory = max_tiles_in_memory
        self.enable_late_data_handling = enable_late_data_handling


class TiledTransformation(Transformation):
    """
    A transformation that operates on tiled data for efficient streaming processing.
    
    Based on Chronon's tiled architecture approach, this transformation divides time
    into manageable chunks (tiles) for processing streaming aggregations and derived
    features efficiently. This transformation works with ComputeEngine for execution.
    
    This is particularly useful for:
    - Temporal aggregations over sliding windows
    - Chaining features across different time horizons
    - Handling late-arriving data in streaming scenarios
    - Memory-efficient processing of large time-series data
    
    Attributes:
        tile_config: Configuration for tiling behavior
        sources: Source feature views or data sources for DAG construction
        schema: Output feature schema for UI rendering
        aggregations: List of Aggregation objects for window-based aggregations
        aggregation_functions: Custom functions to apply within each tile
        chaining_functions: Functions for chaining results across tiles
    """
    
    def __init__(
        self,
        mode: Union[TransformationMode, str],
        udf: Callable[[Any], Any],
        udf_string: str,
        tile_config: TileConfiguration,
        sources: Optional[List[Union[str, "FeatureView", "DataSource"]]] = None,
        schema: Optional[List["Field"]] = None,
        aggregations: Optional[List["Aggregation"]] = None,
        name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        description: str = "",
        owner: str = "",
        aggregation_functions: Optional[List[Callable]] = None,
        chaining_functions: Optional[List[Callable]] = None,
    ):
        super().__init__(
            mode=mode,
            udf=udf,
            udf_string=udf_string,
            name=name,
            tags=tags,
            description=description,
            owner=owner,
        )
        self.tile_config = tile_config
        self.sources = sources or []
        self.schema = schema or []
        self.aggregations = aggregations or []  # Support Feast Aggregation objects
        self.aggregation_functions = aggregation_functions or []
        self.chaining_functions = chaining_functions or []
        
        # State for tracking tiles in memory
        self._tile_cache: Dict[str, Any] = {}
        self._tile_timestamps: List[str] = []
    
    def transform(self, data: Any, timestamp_column: str = "timestamp") -> Any:
        """
        Apply tiled transformation to streaming data.
        
        This method should be executed by a ComputeEngine for proper distributed processing.
        
        Args:
            data: Input streaming data (DataFrame, etc.)
            timestamp_column: Name of the timestamp column for tiling
            
        Returns:
            Transformed data with tiled processing applied
        """
        return self._apply_tiled_processing(data, timestamp_column)
    
    def _apply_tiled_processing(self, data: Any, timestamp_column: str) -> Any:
        """
        Core tiling logic that divides data into time-based tiles and processes them.
        
        This implementation provides the framework for tiled processing. Specific
        compute engines can override this for optimized execution.
        """
        
        # 1. Partition data into tiles based on timestamp
        tiles = self._partition_into_tiles(data, timestamp_column)
        
        # 2. Process each tile with aggregations
        processed_tiles = []
        for tile_key, tile_data in tiles.items():
            processed_tile = self._process_single_tile(tile_data, tile_key)
            processed_tiles.append(processed_tile)
        
        # 3. Chain results across tiles if needed
        if self.chaining_functions:
            return self._chain_tile_results(processed_tiles)
        
        # 4. Combine tiles into final result
        return self._combine_tiles(processed_tiles)
    
    def _partition_into_tiles(self, data: Any, timestamp_column: str) -> Dict[str, Any]:
        """
        Partition input data into time-based tiles.
        
        This is a base implementation - specific compute engines should override
        this for optimized partitioning (e.g., using Spark windowing).
        """
        # Implementation would depend on the specific data processing engine
        # For now, return the data as a single tile
        return {"tile_0": data}
    
    def _process_single_tile(self, tile_data: Any, tile_key: str) -> Any:
        """
        Process a single tile of data, applying aggregations and transformations.
        """
        # Apply Feast Aggregation objects first if any
        result = tile_data
        for aggregation in self.aggregations:
            result = self._apply_aggregation(result, aggregation)
        
        # Apply the main UDF to the tile
        result = self.udf(result)
        
        # Apply any tile-specific aggregation functions
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
    
    def _apply_aggregation(self, data: Any, aggregation: "Aggregation") -> Any:
        """
        Apply a Feast Aggregation object to tile data.
        
        This is a placeholder implementation - specific compute engines should
        override this to properly execute aggregations.
        """
        # Base implementation - just return data unchanged
        # Specific engines (Spark, Ray, etc.) should override this
        return data
    
    def _chain_tile_results(self, processed_tiles: List[Any]) -> Any:
        """
        Chain results across tiles for derived features.
        """
        result = processed_tiles[0] if processed_tiles else None
        
        for chain_func in self.chaining_functions:
            for i in range(1, len(processed_tiles)):
                result = chain_func(result, processed_tiles[i])
        
        return result
    
    def _combine_tiles(self, processed_tiles: List[Any]) -> Any:
        """
        Combine processed tiles into final result.
        This is a placeholder - specific implementations depend on the data format.
        """
        # For now, return the last tile (most recent)
        return processed_tiles[-1] if processed_tiles else None
    
    def infer_features(self, *args, **kwargs) -> Any:
        """
        Infer feature schema from tiled transformation.
        """
        # If schema is explicitly provided, use it
        if self.schema:
            return self.schema
        
        # Otherwise delegate to parent implementation
        return super().infer_features(*args, **kwargs)


# Factory function for creating tiled transformations
def tiled_transformation(
    tile_size: timedelta,
    sources: Optional[List[Union[str, "FeatureView", "DataSource"]]] = None,
    schema: Optional[List["Field"]] = None,
    aggregations: Optional[List["Aggregation"]] = None,
    window_size: Optional[timedelta] = None,
    overlap: Optional[timedelta] = None,
    max_tiles_in_memory: int = 10,  
    enable_late_data_handling: bool = True,
    aggregation_functions: Optional[List[Callable]] = None,
    chaining_functions: Optional[List[Callable]] = None,
    name: Optional[str] = None,
    tags: Optional[Dict[str, str]] = None,
    description: str = "",
    owner: str = "",
):
    """
    Decorator for creating tiled transformations that work with ComputeEngine.
    
    The mode is not specified here - it will be determined by the StreamFeatureView
    or FeatureView that uses this transformation, allowing it to work with different
    compute engines (Spark, Ray, etc.).
    
    Args:
        tile_size: The size of each time tile (e.g., timedelta(hours=1))
        sources: List of source feature views or data sources for DAG construction
        schema: List of Field definitions specifying output feature names and data types
        aggregations: List of Feast Aggregation objects for window-based aggregations
        window_size: The window size for aggregations within tiles
        overlap: Optional overlap between tiles for continuity
        max_tiles_in_memory: Maximum number of tiles to keep in memory
        enable_late_data_handling: Whether to handle late-arriving data
        aggregation_functions: Custom functions to apply within each tile
        chaining_functions: Functions for chaining results across tiles
        name: Optional name for the transformation
        tags: Optional metadata tags
        description: Optional description
        owner: Optional owner
    
    Example:
        @tiled_transformation(
            tile_size=timedelta(hours=1),
            sources=["transaction_source_fv"],
            schema=[
                Field(name="rolling_avg", dtype=Float64),
                Field(name="cumulative_amount", dtype=Float64),
            ],
            aggregations=[
                Aggregation(column="amount", function="sum", time_window=timedelta(minutes=30))
            ],
            window_size=timedelta(minutes=30),
            overlap=timedelta(minutes=5),
        )
        def my_tiled_feature(df):
            return df.assign(
                rolling_avg=df['value'].rolling(window=10).mean(),
                cumulative_amount=df['value'].cumsum()
            )
        
        # Use with StreamFeatureView (mode determined by StreamFeatureView)
        stream_fv = StreamFeatureView(
            name="transaction_features", 
            feature_transformation=my_tiled_feature,
            source=kafka_source,
            mode="spark",  # Mode specified here, not in the transformation
            entities=["customer_id"]
        )
    """
    def decorator(user_function):
        import dill
        
        def mainify(obj):
            # Needed to allow dill to properly serialize the udf
            if obj.__module__ != "__main__":
                obj.__module__ = "__main__"
        
        mainify(user_function)
        udf_string = dill.source.getsource(user_function)
        tile_config = TileConfiguration(
            tile_size=tile_size,
            window_size=window_size,
            overlap=overlap,
            max_tiles_in_memory=max_tiles_in_memory,
            enable_late_data_handling=enable_late_data_handling,
        )
        
        # Create a tiled transformation that can work with any mode
        # The actual mode will be determined when used in StreamFeatureView/FeatureView
        return TiledTransformation(
            mode=TransformationMode.TILING,  # This is our new tiling mode
            udf=user_function,
            udf_string=udf_string,
            tile_config=tile_config,
            sources=sources,
            schema=schema,
            aggregations=aggregations,
            name=name or user_function.__name__,
            tags=tags,
            description=description,
            owner=owner,
            aggregation_functions=aggregation_functions,
            chaining_functions=chaining_functions,
        )
    
    return decorator