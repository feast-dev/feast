"""
Tiling for efficient time-windowed aggregations.

This module provides tiling algorithms and interfaces
that can be implemented by any compute engine (Spark, Ray, etc.).

Architecture:
1. Engine nodes: Convert to pandas (e.g., dataset.to_pandas(), toPandas())
2. orchestrator.py: Generate cumulative tiles
3. tile_subtraction.py: Convert cumulative tiles to windowed aggregations
4. Engine nodes: Convert back to engine format (e.g., from_pandas(), createDataFrame())
"""

from feast.infra.tiling.base import IRMetadata, get_ir_metadata_for_aggregation
from feast.infra.tiling.orchestrator import apply_sawtooth_window_tiling
from feast.infra.tiling.tile_subtraction import (
    convert_cumulative_to_windowed,
    deduplicate_keep_latest,
)

__all__ = [
    "IRMetadata",
    "get_ir_metadata_for_aggregation",
    "apply_sawtooth_window_tiling",
    "convert_cumulative_to_windowed",
    "deduplicate_keep_latest",
]
