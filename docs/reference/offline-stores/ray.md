# Ray Offline Store (contrib)

> **⚠️ Contrib Plugin:**  
> The Ray offline store is a contributed plugin. It may not be as stable or fully supported as core offline stores. Use with caution in production and report issues to the Feast community.

The Ray offline store is a data I/O implementation that leverages [Ray](https://www.ray.io/) for reading and writing data from various sources. It focuses on efficient data access operations, while complex feature computation is handled by the [Ray Compute Engine](../compute-engine/ray.md).

## Overview

The Ray offline store provides:
- Ray-based data reading from file sources (Parquet, CSV, etc.)
- Support for both local and distributed Ray clusters
- Integration with various storage backends (local files, S3, GCS, HDFS)
- Efficient data filtering and column selection
- Timestamp-based data processing with timezone awareness


## Functionality Matrix


| Method                          | Supported |
|----------------------------------|-----------|
| get_historical_features         | Yes       |
| pull_latest_from_table_or_query | Yes       |
| pull_all_from_table_or_query    | Yes       |
| offline_write_batch             | Yes       |
| write_logged_features           | Yes       |


| RetrievalJob Feature            | Supported |
|----------------------------------|-----------|
| export to dataframe             | Yes       |
| export to arrow table           | Yes       |
| persist results in offline store| Yes       |
| local execution of ODFVs        | Yes       |
| remote execution of ODFVs       | No        |
| preview query plan              | Yes       |
| read partitioned data           | Yes       |


## ⚠️ Important: Resource Management

**By default, Ray will use all available system resources (CPU and memory).** This can cause issues in test environments or when experimenting locally, potentially leading to system crashes or unresponsiveness.

**For testing and local experimentation, we strongly recommend:**

1. **Configure resource limits** in your `feature_store.yaml` (see [Resource Management and Testing](#resource-management-and-testing) section below)

This will limit Ray to safe resource levels for testing and development.


## Architecture

The Ray offline store follows Feast's architectural separation:
- **Ray Offline Store**: Handles data I/O operations (reading/writing data)
- **Ray Compute Engine**: Handles complex feature computation and joins
- **Clear Separation**: Each component has a single responsibility

For complex feature processing, historical feature retrieval, and distributed joins, use the [Ray Compute Engine](../compute-engine/ray.md).

## Configuration

The Ray offline store can be configured in your `feature_store.yaml` file. Below are two main configuration patterns:

### Basic Ray Offline Store

For simple data I/O operations without distributed processing:

```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: ray
    storage_path: data/ray_storage        # Optional: Path for storing datasets
    ray_address: localhost:10001          # Optional: Ray cluster address
    use_ray_cluster: false                # Optional: Whether to use Ray cluster
```

### Ray Offline Store + Compute Engine

For distributed feature processing with advanced capabilities:

```yaml
project: my_project
registry: data/registry.db
provider: local

# Ray offline store for data I/O operations
offline_store:
    type: ray
    storage_path: s3://my-bucket/feast-data    # Optional: Path for storing datasets
    ray_address: localhost:10001               # Optional: Ray cluster address
    use_ray_cluster: true                      # Optional: Use Ray cluster mode

# Ray compute engine for distributed feature processing
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
    enable_distributed_joins: true            # Enable distributed joins
    
    # Ray cluster configuration (optional)
    ray_address: localhost:10001               # Ray cluster address
    use_ray_cluster: true                      # Use Ray cluster mode
    staging_location: s3://my-bucket/staging   # Remote staging location
```

### Local Development Configuration

For local development and testing:

```yaml
project: my_local_project
registry: data/registry.db
provider: local

offline_store:
    type: ray
    storage_path: ./data/ray_storage
    # Conservative settings for local development
    broadcast_join_threshold_mb: 25
    max_parallelism_multiplier: 1
    target_partition_size_mb: 16
    enable_ray_logging: false
    # Memory constraints to prevent OOM in test/development environments
    ray_conf:
        num_cpus: 1
        object_store_memory: 104857600  # 100MB
        _memory: 524288000              # 500MB

batch_engine:
    type: ray.engine
    max_workers: 2
    enable_optimization: false
```

### Production Configuration

For production deployments with distributed Ray cluster:

```yaml
project: my_production_project
registry: s3://my-bucket/registry.db
provider: local

offline_store:
    type: ray
    storage_path: s3://my-production-bucket/feast-data
    ray_address: "ray://production-head-node:10001"
    use_ray_cluster: true

batch_engine:
    type: ray.engine
    max_workers: 32
    max_parallelism_multiplier: 4
    enable_optimization: true
    broadcast_join_threshold_mb: 50
    target_partition_size_mb: 128
    window_size_for_joins: "30min"
    ray_address: "ray://production-head-node:10001"
    use_ray_cluster: true
    staging_location: s3://my-production-bucket/staging
```

### Configuration Options

#### Ray Offline Store Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `type` | string | Required | Must be `feast.offline_stores.contrib.ray_offline_store.ray.RayOfflineStore` or `ray` |
| `storage_path` | string | None | Path for storing temporary files and datasets |
| `ray_address` | string | None | Address of the Ray cluster (e.g., "localhost:10001") |
| `use_ray_cluster` | boolean | false | Whether to use Ray cluster mode |
| `ray_conf` | dict | None | Ray initialization parameters for resource management (e.g., memory, CPU limits) |

#### Ray Compute Engine Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `type` | string | Required | Must be `ray.engine` |
| `max_workers` | int | CPU count | Maximum number of Ray workers |
| `enable_optimization` | boolean | true | Enable performance optimizations |
| `broadcast_join_threshold_mb` | int | 100 | Size threshold for broadcast joins (MB) |
| `max_parallelism_multiplier` | int | 2 | Parallelism as multiple of CPU cores |
| `target_partition_size_mb` | int | 64 | Target partition size (MB) |
| `window_size_for_joins` | string | "1H" | Time window for distributed joins |
| `enable_distributed_joins` | boolean | true | Enable distributed joins for large datasets |
| `staging_location` | string | None | Remote path for batch materialization jobs |

## Resource Management and Testing

### Overview

**By default, Ray will use all available system resources (CPU and memory).** This can cause issues in test environments or when experimenting locally, potentially leading to system crashes or unresponsiveness.

### Resource Configuration

For custom resource control, configure limits in your `feature_store.yaml`:

#### Conservative Settings (Local Development/Testing)

```yaml
offline_store:
    type: ray
    storage_path: ./data/ray_storage
    # Resource optimization settings
    broadcast_join_threshold_mb: 25        # Smaller datasets for broadcast joins
    max_parallelism_multiplier: 1          # Reduced parallelism  
    target_partition_size_mb: 16           # Smaller partition sizes
    enable_ray_logging: false              # Disable verbose logging
    # Memory constraints to prevent OOM in test environments
    ray_conf:
        num_cpus: 1
        object_store_memory: 104857600      # 100MB
        _memory: 524288000                  # 500MB
```

#### Production Settings

```yaml
offline_store:
    type: ray
    storage_path: s3://my-bucket/feast-data
    ray_address: "ray://production-cluster:10001"
    use_ray_cluster: true
    # Optimized for production workloads
    broadcast_join_threshold_mb: 100
    max_parallelism_multiplier: 2
    target_partition_size_mb: 64
    enable_ray_logging: true
```

### Resource Configuration Options

| Setting | Default | Description | Testing Recommendation |
|---------|---------|-------------|------------------------|
| `broadcast_join_threshold_mb` | 100 | Size threshold for broadcast joins (MB) | 25 |
| `max_parallelism_multiplier` | 2 | Parallelism as multiple of CPU cores | 1 |
| `target_partition_size_mb` | 64 | Target partition size (MB) | 16 |
| `enable_ray_logging` | false | Enable Ray progress bars and logging | false |

### Environment-Specific Recommendations

#### Local Development
```yaml
# feature_store.yaml
offline_store:
    type: ray
    broadcast_join_threshold_mb: 25
    max_parallelism_multiplier: 1
    target_partition_size_mb: 16
```

#### Production Clusters
```yaml
# feature_store.yaml  
offline_store:
    type: ray
    ray_address: "ray://cluster-head:10001"
    use_ray_cluster: true
    broadcast_join_threshold_mb: 200
    max_parallelism_multiplier: 4
```

## Usage Examples

### Basic Data Source Reading

```python
from feast import FeatureStore, FeatureView, FileSource
from feast.types import Float32, Int64
from datetime import timedelta

# Define a feature view
driver_stats = FeatureView(
    name="driver_stats",
    entities=["driver_id"],
    ttl=timedelta(days=1),
    source=FileSource(
        path="data/driver_stats.parquet",
        timestamp_field="event_timestamp",
    ),
    schema=[
        ("driver_id", Int64),
        ("avg_daily_trips", Float32),
    ],
)

# Initialize feature store
store = FeatureStore("feature_store.yaml")

# The Ray offline store handles data I/O operations
# For complex feature computation, use Ray Compute Engine
```

### Direct Data Access

The Ray offline store provides direct access to underlying data:

```python
from feast.infra.offline_stores.contrib.ray_offline_store.ray import RayOfflineStore
from datetime import datetime, timedelta

# Pull latest data from a table
job = RayOfflineStore.pull_latest_from_table_or_query(
    config=store.config,
    data_source=driver_stats.source,
    join_key_columns=["driver_id"],
    feature_name_columns=["avg_daily_trips"],
    timestamp_field="event_timestamp",
    created_timestamp_column=None,
    start_date=datetime.now() - timedelta(days=7),
    end_date=datetime.now(),
)

# Convert to pandas DataFrame
df = job.to_df()
print(f"Retrieved {len(df)} rows")

# Convert to Arrow Table
arrow_table = job.to_arrow()

# Get Ray dataset directly
ray_dataset = job.to_ray_dataset()
```

### Batch Writing

The Ray offline store supports batch writing for materialization:

```python
import pyarrow as pa
from feast import FeatureView

# Create sample data
data = pa.table({
    "driver_id": [1, 2, 3, 4, 5],
    "avg_daily_trips": [10.5, 15.2, 8.7, 12.3, 9.8],
    "event_timestamp": [datetime.now()] * 5
})

# Write batch data
RayOfflineStore.offline_write_batch(
    config=store.config,
    feature_view=driver_stats,
    table=data,
    progress=lambda x: print(f"Wrote {x} rows")
)
```

### Saved Dataset Persistence

The Ray offline store supports persisting datasets for later analysis:

```python
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage

# Create storage destination
storage = SavedDatasetFileStorage(path="data/training_dataset.parquet")

# Persist the dataset
job.persist(storage, allow_overwrite=False)

# Create a saved dataset in the registry
saved_dataset = store.create_saved_dataset(
    from_=job,
    name="driver_training_dataset",
    storage=storage,
    tags={"purpose": "data_access", "version": "v1"}
)

print(f"Saved dataset created: {saved_dataset.name}")
```

### Remote Storage Support

The Ray offline store supports various remote storage backends:

```python
# S3 storage
s3_storage = SavedDatasetFileStorage(path="s3://my-bucket/datasets/driver_features.parquet")
job.persist(s3_storage, allow_overwrite=True)

# Google Cloud Storage
gcs_storage = SavedDatasetFileStorage(path="gs://my-project-bucket/datasets/driver_features.parquet")
job.persist(gcs_storage, allow_overwrite=True)

# HDFS
hdfs_storage = SavedDatasetFileStorage(path="hdfs://namenode:8020/datasets/driver_features.parquet")
job.persist(hdfs_storage, allow_overwrite=True)
```

### Using Ray Cluster

To use Ray in cluster mode for distributed data access:

1. Start a Ray cluster:
```bash
ray start --head --port=10001
```

2. Configure your `feature_store.yaml`:
```yaml
offline_store:
    type: ray
    ray_address: localhost:10001
    use_ray_cluster: true
    storage_path: s3://my-bucket/features
```

3. For multiple worker nodes:
```bash
# On worker nodes
ray start --address='head-node-ip:10001'
```

### Data Source Validation

The Ray offline store validates data sources to ensure compatibility:

```python
from feast.infra.offline_stores.contrib.ray_offline_store.ray import RayOfflineStore

# Validate a data source
try:
    RayOfflineStore.validate_data_source(store.config, driver_stats.source)
    print("Data source is valid")
except Exception as e:
    print(f"Data source validation failed: {e}")
```

## Limitations

The Ray offline store has the following limitations:

1. **File Sources Only**: Currently supports only `FileSource` data sources
2. **No Direct SQL**: Does not support SQL query interfaces
3. **No Online Writes**: Cannot write directly to online stores
4. **Limited Transformations**: Complex feature transformations should use the Ray Compute Engine

## Integration with Ray Compute Engine

For complex feature processing operations, use the Ray offline store in combination with the [Ray Compute Engine](../compute-engine/ray.md). See the **Ray Offline Store + Compute Engine** configuration example in the [Configuration](#configuration) section above for a complete setup.

The Ray offline store provides the data I/O foundation, while the Ray compute engine handles:
- **Point-in-time joins**: Efficient temporal joins for historical feature retrieval
- **Feature aggregations**: Distributed aggregations across time windows
- **Complex transformations**: Advanced feature transformations and computations  
- **Historical feature retrieval**: `get_historical_features()` with distributed processing
- **Distributed processing optimization**: Automatic join strategy selection and resource management
- **Materialization**: Distributed batch materialization with progress tracking


For more advanced troubleshooting, refer to the [Ray documentation](https://docs.ray.io/en/latest/data/getting-started.html).

## Quick Reference

### Configuration Templates

**Basic Ray Offline Store** (local development):
```yaml
offline_store:
    type: ray
    storage_path: ./data/ray_storage
    # Conservative settings for local development
    broadcast_join_threshold_mb: 25
    max_parallelism_multiplier: 1
    target_partition_size_mb: 16
    enable_ray_logging: false
```

**Ray Offline Store + Compute Engine** (distributed processing):
```yaml
offline_store:
    type: ray
    storage_path: s3://my-bucket/feast-data
    use_ray_cluster: true
    
batch_engine:
    type: ray.engine
    max_workers: 8
    enable_optimization: true
    broadcast_join_threshold_mb: 100
```

### Key Commands

```python
# Initialize feature store
store = FeatureStore("feature_store.yaml")

# Get historical features (uses compute engine if configured)
features = store.get_historical_features(entity_df=df, features=["fv:feature"])

# Direct data access (uses offline store)
job = RayOfflineStore.pull_latest_from_table_or_query(...)
df = job.to_df()
```

For complete examples, see the [Configuration](#configuration) section above.