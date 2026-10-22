"""
Ray offline store for Feast.

This module provides distributed offline feature store functionality using Ray with
advanced optimization features for scalable feature retrieval.

Key Features:
- Intelligent join strategy selection (broadcast vs. distributed)
- Resource-aware partitioning and parallelism
- Windowed temporal joins for large datasets
- Configurable performance tuning parameters
- Automatic cluster resource management

Classes:
- RayOfflineStore: Main offline store implementation
- RayOfflineStoreConfig: Configuration with optimization settings
- RayRetrievalJob: Enhanced retrieval job with caching
- RayResourceManager: Cluster resource management
- RayDataProcessor: Optimized data processing operations

Usage:
Configure in your feature_store.yaml:
```yaml
offline_store:
    type: ray
    storage_path: /path/to/storage
    broadcast_join_threshold_mb: 100
    enable_distributed_joins: true
    max_parallelism_multiplier: 2
    target_partition_size_mb: 64
    window_size_for_joins: "1H"
```

Performance Optimizations:
- Broadcast joins for small datasets (<100MB by default)
- Distributed windowed joins for large datasets
- Optimal partitioning based on cluster resources
- Memory-aware buffer sizing
- Lazy evaluation with caching
"""

from .ray import (
    RayDataProcessor,
    RayOfflineStore,
    RayOfflineStoreConfig,
    RayResourceManager,
    RayRetrievalJob,
)

__all__ = [
    "RayOfflineStore",
    "RayOfflineStoreConfig",
    "RayRetrievalJob",
    "RayResourceManager",
    "RayDataProcessor",
]
