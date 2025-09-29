# Tiled Streaming Features Example

This example demonstrates how to use Feast's tiling transformation engine for efficient streaming feature engineering.

## Overview

Tiling in Feast is inspired by Chronon's tiled architecture and provides:
- Time-based data partitioning into manageable tiles
- Efficient temporal aggregations over sliding windows  
- Chaining features across different time horizons
- Memory-efficient processing of streaming data
- Late-arriving data handling

## Examples

See the example files:
- `basic_tiling.py` - Basic tiled transformation usage
- `advanced_tiling.py` - Advanced features like chaining and complex aggregations
- `streaming_integration.py` - Integration with StreamFeatureView

## Running the Examples

```bash
# Basic tiling example
python basic_tiling.py

# Advanced tiling with chaining
python advanced_tiling.py

# Streaming integration (requires Kafka setup)
python streaming_integration.py
```

## Key Concepts

### Tile Configuration
- **tile_size**: Duration of each time tile (e.g., `timedelta(hours=1)`)
- **overlap**: Optional overlap between tiles for continuity
- **max_tiles_in_memory**: Maximum number of tiles to keep in memory
- **enable_late_data_handling**: Whether to handle late-arriving data

### Aggregation Functions
Functions that operate within each tile to compute aggregated features.

### Chaining Functions
Functions that chain results across tiles for derived features that require continuity across time boundaries.