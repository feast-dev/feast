# Ray Offline Store (contrib)

The Ray offline store is a distributed offline store implementation that leverages [Ray](https://www.ray.io/) for distributed data processing. It's particularly useful for large-scale feature engineering and retrieval operations.

## Overview

The Ray offline store provides:
- Distributed data processing using Ray
- Support for both local and cluster modes
- Efficient data loading and processing
- Integration with various storage backends (local files, S3, etc.)
- Support for scalable batch materialization
- Saved dataset persistence for data analysis and model training

## Configuration

The Ray offline store can be configured in your `feature_store.yaml` file:

```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: ray
    storage_path: data/ray_storage  # Optional: Path for materialized data
    ray_address: localhost:10001    # Optional: Ray cluster address
    use_ray_cluster: false          # Optional: Whether to use Ray cluster
```

### Configuration Options

| Option | Type | Required | Description |
|--------|------|----------|-------------|
| `type` | string | Yes | Must be `feast.offline_stores.ray.RayOfflineStore` |
| `storage_path` | string | No | Path for storing materialized data (e.g., "s3://my-bucket/data") |
| `ray_address` | string | No | Address of the Ray cluster (e.g., "localhost:10001") |
| `use_ray_cluster` | boolean | No | Whether to use Ray cluster mode (default: false) |

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

To use Ray in cluster mode:

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
