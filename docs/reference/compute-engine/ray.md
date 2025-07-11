# Ray Compute Engine (contrib)

The Ray compute engine is a distributed compute implementation that leverages [Ray](https://www.ray.io/) for executing feature pipelines including transformations, aggregations, joins, and materializations. It provides scalable and efficient distributed processing for both `materialize()` and `get_historical_features()` operations.

## Overview

The Ray compute engine provides:
- **Distributed DAG Execution**: Executes feature computation DAGs across Ray clusters
- **Intelligent Join Strategies**: Automatic selection between broadcast and distributed joins
- **Lazy Evaluation**: Deferred execution for optimal performance
- **Resource Management**: Automatic scaling and resource optimization
- **Point-in-Time Joins**: Efficient temporal joins for historical feature retrieval

## Architecture

The Ray compute engine follows Feast's DAG-based architecture:

```
EntityDF → RayReadNode → RayJoinNode → RayFilterNode → RayAggregationNode → RayTransformationNode → Output
```

### Core Components

| Component | Description |
|-----------|-------------|
| `RayComputeEngine` | Main engine implementing `ComputeEngine` interface |
| `RayFeatureBuilder` | Constructs DAG from Feature View definitions |
| `RayDAGNode` | Ray-specific DAG node implementations |
| `RayDAGRetrievalJob` | Executes retrieval plans and returns results |
| `RayMaterializationJob` | Handles materialization job tracking |

## Configuration

Configure the Ray compute engine in your `feature_store.yaml`:

```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: ray
    storage_path: data/ray_storage
batch_engine:
    type: ray.engine
    max_workers: 4                          # Optional: Maximum number of workers
    enable_optimization: true               # Optional: Enable performance optimizations
    broadcast_join_threshold_mb: 100       # Optional: Broadcast join threshold (MB)
    max_parallelism_multiplier: 2          # Optional: Parallelism multiplier
    target_partition_size_mb: 64           # Optional: Target partition size (MB)
    window_size_for_joins: "1H"            # Optional: Time window for distributed joins
    ray_address: localhost:10001           # Optional: Ray cluster address
    use_ray_cluster: false                 # Optional: Use Ray cluster mode
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `type` | string | Required | Must be `ray.engine` |
| `max_workers` | int | CPU count | Maximum number of Ray workers |
| `enable_optimization` | boolean | true | Enable performance optimizations |
| `broadcast_join_threshold_mb` | int | 100 | Size threshold for broadcast joins (MB) |
| `max_parallelism_multiplier` | int | 2 | Parallelism as multiple of CPU cores |
| `target_partition_size_mb` | int | 64 | Target partition size (MB) |
| `window_size_for_joins` | string | "1H" | Time window for distributed joins |
| `ray_address` | string | None | Ray cluster address |
| `use_ray_cluster` | boolean | false | Use Ray cluster mode |

## Usage Examples

### Basic Historical Feature Retrieval

```python
from feast import FeatureStore
import pandas as pd
from datetime import datetime

# Initialize feature store with Ray compute engine
store = FeatureStore("feature_store.yaml")

# Create entity DataFrame
entity_df = pd.DataFrame({
    "driver_id": [1, 2, 3, 4, 5],
    "event_timestamp": [datetime.now()] * 5
})

# Get historical features using Ray compute engine
features = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_stats:avg_daily_trips",
        "driver_stats:total_distance"
    ]
)

# Convert to DataFrame
df = features.to_df()
print(f"Retrieved {len(df)} rows with {len(df.columns)} columns")
```

### Batch Materialization

```python
from datetime import datetime, timedelta

# Materialize features using Ray compute engine
store.materialize(
    start_date=datetime.now() - timedelta(days=7),
    end_date=datetime.now(),
    feature_views=["driver_stats", "customer_stats"]
)

# The Ray compute engine handles:
# - Distributed data processing
# - Optimal join strategies
# - Resource management
# - Progress tracking
```

### Large-Scale Feature Retrieval

```python
# Handle large entity datasets efficiently
large_entity_df = pd.DataFrame({
    "driver_id": range(1, 1000000),  # 1M entities
    "event_timestamp": [datetime.now()] * 1000000
})

# Ray compute engine automatically:
# - Partitions data optimally
# - Selects appropriate join strategies
# - Distributes computation across cluster
features = store.get_historical_features(
    entity_df=large_entity_df,
    features=[
        "driver_stats:avg_daily_trips",
        "driver_stats:total_distance",
        "customer_stats:lifetime_value"
    ]
).to_df()
```

### Advanced Configuration

```yaml
# Production-ready configuration
batch_engine:
    type: ray.engine
    # Resource configuration
    max_workers: 16
    max_parallelism_multiplier: 4
    
    # Performance optimization
    enable_optimization: true
    broadcast_join_threshold_mb: 50
    target_partition_size_mb: 128
    
    # Distributed join configuration
    window_size_for_joins: "30min"
    
    # Ray cluster configuration
    use_ray_cluster: true
    ray_address: "ray://head-node:10001"
```

### Complete Example Configuration

Here's a complete example configuration showing how to use Ray offline store with Ray compute engine:

```yaml
# Complete example configuration for Ray offline store + Ray compute engine
# This shows how to use both components together for distributed processing

project: my_feast_project
registry: data/registry.db
provider: local

# Ray offline store configuration
# Handles data I/O operations (reading/writing data)
offline_store:
    type: ray
    storage_path: s3://my-bucket/feast-data    # Optional: Path for storing datasets
    ray_address: localhost:10001               # Optional: Ray cluster address
    use_ray_cluster: true                      # Optional: Use Ray cluster mode

# Ray compute engine configuration  
# Handles complex feature computation and distributed processing
batch_engine:
    type: ray.engine
    
    # Resource configuration
    max_workers: 8                             # Maximum number of Ray workers
    max_parallelism_multiplier: 2              # Parallelism as multiple of CPU cores
    
    # Performance optimization
    enable_optimization: true                  # Enable performance optimizations
    broadcast_join_threshold_mb: 100           # Broadcast join threshold (MB)
    target_partition_size_mb: 64               # Target partition size (MB)
    
    # Distributed join configuration
    window_size_for_joins: "1H"                # Time window for distributed joins
    
    # Ray cluster configuration (inherits from offline_store if not specified)
    ray_address: localhost:10001               # Ray cluster address
    use_ray_cluster: true                      # Use Ray cluster mode

# Optional: Online store configuration
online_store:
    type: sqlite
    path: data/online_store.db

# Optional: Feature server configuration
feature_server:
    port: 6566
    metrics_port: 8888
```

## DAG Node Types

The Ray compute engine implements several specialized DAG nodes:

### RayReadNode

Reads data from Ray-compatible sources:
- Supports Parquet, CSV, and other formats
- Handles partitioning and schema inference
- Applies field mappings and filters

### RayJoinNode

Performs distributed joins:
- **Broadcast Join**: For small datasets (<100MB)
- **Distributed Join**: For large datasets with time-based windowing
- **Automatic Strategy Selection**: Based on dataset size and cluster resources

### RayFilterNode

Applies filters and time-based constraints:
- TTL-based filtering
- Timestamp range filtering
- Custom predicate filtering

### RayAggregationNode

Handles feature aggregations:
- Windowed aggregations
- Grouped aggregations
- Custom aggregation functions

### RayTransformationNode

Applies feature transformations:
- Row-level transformations
- Column-level transformations
- Custom transformation functions

### RayWriteNode

Writes results to various targets:
- Online stores
- Offline stores
- Temporary storage

## Join Strategies

The Ray compute engine automatically selects optimal join strategies:

### Broadcast Join

Used for small feature datasets:
```python
# Automatically selected when feature data < 100MB
# Features are cached in Ray's object store
# Entities are distributed across cluster
# Each worker gets a copy of feature data
```

### Distributed Windowed Join

Used for large feature datasets:
```python
# Automatically selected when feature data > 100MB
# Data is partitioned by time windows
# Point-in-time joins within each window
# Results are combined across windows
```

### Strategy Selection Logic

```python
def select_join_strategy(feature_size_mb, threshold_mb):
    if feature_size_mb < threshold_mb:
        return "broadcast"
    else:
        return "distributed_windowed"
```

## Performance Optimization

### Automatic Optimization

The Ray compute engine includes several automatic optimizations:

1. **Partition Optimization**: Automatically determines optimal partition sizes
2. **Join Strategy Selection**: Chooses between broadcast and distributed joins
3. **Resource Allocation**: Scales workers based on available resources
4. **Memory Management**: Handles out-of-core processing for large datasets

### Manual Tuning

For specific workloads, you can fine-tune performance:

```yaml
batch_engine:
    type: ray.engine
    # Fine-tuning for high-throughput scenarios
    broadcast_join_threshold_mb: 200      # Larger broadcast threshold
    max_parallelism_multiplier: 1        # Conservative parallelism
    target_partition_size_mb: 512        # Larger partitions
    window_size_for_joins: "2H"          # Larger time windows
```

### Monitoring and Metrics

Monitor Ray compute engine performance:

```python
import ray

# Check cluster resources
resources = ray.cluster_resources()
print(f"Available CPUs: {resources.get('CPU', 0)}")
print(f"Available memory: {resources.get('memory', 0) / 1e9:.2f} GB")

# Monitor job progress
job = store.get_historical_features(...)
# Ray compute engine provides built-in progress tracking
```

## Integration Examples

### With Spark Offline Store

```yaml
# Use Ray compute engine with Spark offline store
offline_store:
    type: spark
    spark_conf:
        spark.executor.memory: "4g"
        spark.executor.cores: "2"
batch_engine:
    type: ray.engine
    max_workers: 8
    enable_optimization: true
```

### With Cloud Storage

```yaml
# Use Ray compute engine with cloud storage
offline_store:
    type: ray
    storage_path: s3://my-bucket/feast-data
batch_engine:
    type: ray.engine
    use_ray_cluster: true
    ray_address: "ray://ray-cluster:10001"
    broadcast_join_threshold_mb: 50
```

### With Feature Transformations

```python
from feast import FeatureView, Field
from feast.types import Float64
from feast.on_demand_feature_view import on_demand_feature_view

@on_demand_feature_view(
    sources=["driver_stats"],
    schema=[Field(name="trips_per_hour", dtype=Float64)]
)
def trips_per_hour(features_df):
    features_df["trips_per_hour"] = features_df["avg_daily_trips"] / 24
    return features_df

# Ray compute engine handles transformations efficiently
features = store.get_historical_features(
    entity_df=entity_df,
    features=["trips_per_hour:trips_per_hour"]
)
```

For more information, see the [Ray documentation](https://docs.ray.io/en/latest/) and [Ray Data guide](https://docs.ray.io/en/latest/data/getting-started.html). 