# Ray Offline Store (contrib)

The Ray offline store is a distributed offline store implementation that leverages [Ray](https://www.ray.io/) for distributed data processing. It's particularly useful for large-scale feature engineering and retrieval operations.

## Overview

The Ray offline store provides:
- Distributed data processing using Ray
- Support for both local and cluster modes
- Integration with various storage backends (local files, S3, etc.)
- Support for scalable batch materialization
- Saved dataset persistence for data analysis and model training

## Optimization Features

### Intelligent Join Strategies

The Ray offline store now includes intelligent join strategy selection:

- **Broadcast Joins**: For small feature datasets (<100MB by default), data is stored in Ray's object store for efficient broadcasting
- **Distributed Windowed Joins**: For large datasets, uses time-based windowing for distributed point-in-time joins
- **Automatic Strategy Selection**: Chooses optimal join strategy based on dataset size and cluster resources

### Resource Management

The store automatically detects and optimizes for your Ray cluster:

- **Auto-scaling**: Adjusts parallelism based on available CPU cores
- **Memory Optimization**: Configures buffer sizes based on available memory
- **Partition Optimization**: Calculates optimal partition sizes for your workload

## Configuration

The Ray offline store can be configured in your `feature_store.yaml` file:

```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: ray
    storage_path: data/ray_storage        # Optional: Path for materialized data
    ray_address: localhost:10001          # Optional: Ray cluster address
    use_ray_cluster: false                # Optional: Whether to use Ray cluster
    # New optimization settings
    broadcast_join_threshold_mb: 100      # Optional: Threshold for broadcast joins (MB)
    enable_distributed_joins: true       # Optional: Enable distributed join strategies
    max_parallelism_multiplier: 2        # Optional: Max parallelism as multiple of CPU cores
    target_partition_size_mb: 64         # Optional: Target partition size (MB)
    window_size_for_joins: "1H"          # Optional: Time window size for distributed joins
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `type` | string | Required | Must be `feast.offline_stores.contrib.ray_offline_store.ray.RayOfflineStore` or `ray` |
| `storage_path` | string | None | Path for storing materialized data (e.g., "s3://my-bucket/data") |
| `ray_address` | string | None | Address of the Ray cluster (e.g., "localhost:10001") |
| `use_ray_cluster` | boolean | false | Whether to use Ray cluster mode |
| `broadcast_join_threshold_mb` | int | 100 | Size threshold (MB) below which broadcast joins are used |
| `enable_distributed_joins` | boolean | true | Enable intelligent distributed join strategies |
| `max_parallelism_multiplier` | int | 2 | Maximum parallelism as multiple of CPU cores |
| `target_partition_size_mb` | int | 64 | Target size for data partitions (MB) |
| `window_size_for_joins` | string | "1H" | Time window size for distributed temporal joins |

## Usage Examples

### Basic Usage

```python
from feast import FeatureStore, FeatureView, FileSource
from feast.types import Float32, Int64
from datetime import timedelta

# Define a feature view
driver_stats = FeatureView(
    name="driver_stats",
    entities=["driver_id"],
    ttl=timedelta(days=1),
    online=True,
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

# Get historical features
entity_df = pd.DataFrame({
    "driver_id": [1, 2, 3],
    "event_timestamp": [datetime.now()] * 3
})

features = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_stats:avg_daily_trips"
    ]
).to_df()
```

### Optimized Configuration for Large Datasets

```yaml
offline_store:
    type: ray
    storage_path: s3://my-bucket/feast-data
    use_ray_cluster: true
    ray_address: ray://head-node:10001
    # Optimize for large datasets
    broadcast_join_threshold_mb: 50       # Smaller threshold for large clusters
    max_parallelism_multiplier: 4        # Higher parallelism for more CPUs
    target_partition_size_mb: 128        # Larger partitions for better throughput
    window_size_for_joins: "30min"       # Smaller windows for better distribution
```

### High-Performance Feature Retrieval

```python
# For large-scale feature retrieval with millions of entities
large_entity_df = pd.DataFrame({
    "driver_id": range(1, 1000000),  # 1M drivers
    "event_timestamp": [datetime.now()] * 1000000
})

# The Ray offline store will automatically:
# 1. Detect large dataset and use distributed joins
# 2. Partition data optimally across cluster
# 3. Use appropriate join strategy based on feature data size
features = store.get_historical_features(
    entity_df=large_entity_df,
    features=[
        "driver_stats:avg_daily_trips",
        "driver_stats:total_distance"
    ]
).to_df()
```

### Saved Dataset Persistence

The Ray offline store supports persisting datasets for later analysis and model training:

```python
from feast import FeatureStore
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage

# Initialize feature store
store = FeatureStore("feature_store.yaml")

# Get historical features
entity_df = pd.DataFrame({
    "driver_id": [1, 2, 3, 4, 5],
    "event_timestamp": [datetime.now()] * 5
})

# Create a retrieval job
job = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_stats:avg_daily_trips",
        "driver_stats:total_trips"
    ]
)

# Create storage destination
storage = SavedDatasetFileStorage(path="data/training_dataset.parquet")

# Persist the dataset
job.persist(storage, allow_overwrite=False)

# Create a saved dataset in the registry
saved_dataset = store.create_saved_dataset(
    from_=job,
    name="driver_training_dataset",
    storage=storage,
    tags={"purpose": "model_training", "version": "v1"}
)

print(f"Saved dataset created: {saved_dataset.name}")
```

### Remote Storage Persistence

You can persist datasets to remote storage for distributed access:

```python
# Persist to S3
s3_storage = SavedDatasetFileStorage(path="s3://my-bucket/datasets/driver_features.parquet")
job.persist(s3_storage, allow_overwrite=True)

# Persist to Google Cloud Storage
gcs_storage = SavedDatasetFileStorage(path="gs://my-project-bucket/datasets/driver_features.parquet")
job.persist(gcs_storage, allow_overwrite=True)

# Persist to HDFS
hdfs_storage = SavedDatasetFileStorage(path="hdfs://namenode:8020/datasets/driver_features.parquet")
job.persist(hdfs_storage, allow_overwrite=True)
```

### Retrieving Saved Datasets

You can retrieve previously saved datasets:

```python
# Retrieve a saved dataset
saved_dataset = store.get_saved_dataset("driver_training_dataset")

# Convert to different formats
df = saved_dataset.to_df()  # Pandas DataFrame
arrow_table = saved_dataset.to_arrow()  # PyArrow Table

# Get dataset metadata
print(f"Dataset features: {saved_dataset.features}")
print(f"Join keys: {saved_dataset.join_keys}")
print(f"Min timestamp: {saved_dataset.min_event_timestamp}")
print(f"Max timestamp: {saved_dataset.max_event_timestamp}")
```

### Batch Materialization with Persistence

Combine batch materialization with dataset persistence:

```python
from datetime import datetime, timedelta

# Materialize features for the last 30 days
store.materialize(
    start_date=datetime.now() - timedelta(days=30),
    end_date=datetime.now(),
    feature_views=["driver_stats"]
)

# Get historical features for the materialized period
entity_df = pd.DataFrame({
    "driver_id": list(range(1, 1001)),  # 1000 drivers
    "event_timestamp": [datetime.now()] * 1000
})

job = store.get_historical_features(
    entity_df=entity_df,
    features=["driver_stats:avg_daily_trips"]
)

# Persist to remote storage for distributed access
remote_storage = SavedDatasetFileStorage(
    path="s3://my-bucket/large_datasets/driver_features_30d.parquet"
)
job.persist(remote_storage, allow_overwrite=True)
```

### Using Ray Cluster

To use Ray in cluster mode for maximum performance:

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
    # Cluster-optimized settings
    max_parallelism_multiplier: 3
    target_partition_size_mb: 256
```

3. For multiple worker nodes:
```bash
# On worker nodes
ray start --address='head-node-ip:10001'
```

### Remote Storage

For large-scale materialization, you can use remote storage:

```yaml
offline_store:
    type: ray
    storage_path: s3://my-bucket/features
```

```python
# Materialize features to remote storage
store.materialize(
    start_date=datetime.now() - timedelta(days=7),
    end_date=datetime.now(),
    feature_views=["driver_stats"]
)
```


### Custom Optimization

For specific workloads, you can fine-tune the configuration:

```yaml
offline_store:
    type: ray
    # Fine-tuning for high-throughput scenarios
    broadcast_join_threshold_mb: 200      # Larger broadcast threshold
    max_parallelism_multiplier: 1        # Conservative parallelism
    target_partition_size_mb: 512        # Larger partitions
    window_size_for_joins: "2H"          # Larger time windows
```
