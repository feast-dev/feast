# Ray Offline Store (contrib)

> **⚠️ Contrib Plugin:**  
> The Ray offline store is a contributed plugin. It may not be as stable or fully supported as core offline stores. Use with caution in production and report issues to the Feast community.

The Ray offline store is a data I/O implementation that leverages [Ray](https://www.ray.io/) for reading and writing data from various sources. It focuses on efficient data access operations, while complex feature computation is handled by the [Ray Compute Engine](../compute-engine/ray.md).

## Quick Start with Ray Template

The easiest way to get started with Ray offline store is to use the built-in Ray template:

```bash
feast init -t ray my_ray_project
cd my_ray_project/feature_repo
```

This template includes:
- Pre-configured Ray offline store and compute engine setup
- Sample feature definitions optimized for Ray processing
- Demo workflow showcasing Ray capabilities
- Resource settings for local development

The template provides a complete working example with sample datasets and demonstrates both Ray offline store data I/O operations and Ray compute engine distributed processing.

## Overview

The Ray offline store provides:
- Ray-based data reading from file sources (Parquet, CSV, etc.)
- Support for local, remote, and KubeRay (Kubernetes-managed) clusters
- Integration with various storage backends (local files, S3, GCS, HDFS, Azure Blob)
- Efficient data filtering and column selection
- Timestamp-based data processing with timezone awareness
- Enterprise-ready KubeRay cluster support via CodeFlare SDK


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

The Ray offline store can be configured in your `feature_store.yaml` file. It supports **three execution modes**:

1. **LOCAL**: Ray runs locally on the same machine (default)
2. **REMOTE**: Connects to a remote Ray cluster via `ray_address`
3. **KUBERAY**: Connects to Ray clusters on Kubernetes via CodeFlare SDK

### Execution Modes

#### Local Mode (Default)

For simple data I/O operations without distributed processing:

```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store:
    type: ray
    storage_path: data/ray_storage        # Optional: Path for storing datasets
```

#### Remote Ray Cluster

Connect to an existing Ray cluster:

```yaml
offline_store:
    type: ray
    storage_path: s3://my-bucket/feast-data
    ray_address: "ray://my-cluster.example.com:10001"
```

#### KubeRay Cluster (Kubernetes)

Connect to Ray clusters on Kubernetes using CodeFlare SDK:

```yaml
offline_store:
    type: ray
    storage_path: s3://my-bucket/feast-data
    use_kuberay: true
    kuberay_conf:
        cluster_name: "feast-ray-cluster"
        namespace: "feast-system"
        auth_token: "${RAY_AUTH_TOKEN}"
        auth_server: "https://api.openshift.com:6443"
        skip_tls: false
    enable_ray_logging: false
```

**Environment Variables** (alternative to config file):
```bash
export FEAST_RAY_USE_KUBERAY=true
export FEAST_RAY_CLUSTER_NAME=feast-ray-cluster
export FEAST_RAY_AUTH_TOKEN=your-token
export FEAST_RAY_AUTH_SERVER=https://api.openshift.com:6443
export FEAST_RAY_NAMESPACE=feast-system
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

batch_engine:
    type: ray.engine
    max_workers: 32
    max_parallelism_multiplier: 4
    enable_optimization: true
    broadcast_join_threshold_mb: 50
    target_partition_size_mb: 128
    window_size_for_joins: "30min"
    ray_address: "ray://production-head-node:10001"
    staging_location: s3://my-production-bucket/staging
```

### Configuration Options

#### Ray Offline Store Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `type` | string | Required | Must be `feast.offline_stores.contrib.ray_offline_store.ray.RayOfflineStore` or `ray` |
| `storage_path` | string | None | Path for storing temporary files and datasets |
| `ray_address` | string | None | Ray cluster address (triggers REMOTE mode, e.g., "ray://host:10001") |
| `use_kuberay` | boolean | None | Enable KubeRay mode (overrides ray_address) |
| `kuberay_conf` | dict | None | **KubeRay configuration dict** with keys: `cluster_name` (required), `namespace` (default: "default"), `auth_token`, `auth_server`, `skip_tls` (default: false) |
| `enable_ray_logging` | boolean | false | Enable Ray progress bars and verbose logging |
| `ray_conf` | dict | None | Ray initialization parameters for resource management (e.g., memory, CPU limits) |
| `broadcast_join_threshold_mb` | int | 100 | Size threshold for broadcast joins (MB) |
| `enable_distributed_joins` | boolean | true | Enable distributed joins for large datasets |
| `max_parallelism_multiplier` | int | 2 | Parallelism as multiple of CPU cores |
| `target_partition_size_mb` | int | 64 | Target partition size (MB) |
| `window_size_for_joins` | string | "1H" | Time window for distributed joins |

#### Mode Detection Precedence

The Ray offline store automatically detects the execution mode using the following precedence:

1. **Environment Variables** (highest priority)
   - `FEAST_RAY_USE_KUBERAY`, `FEAST_RAY_CLUSTER_NAME`, etc.
2. **Config `kuberay_conf`**
   - If present → KubeRay mode
3. **Config `ray_address`**
   - If present → Remote mode
4. **Default**
   - Local mode (lowest priority)

#### Ray Compute Engine Options

For Ray compute engine configuration options, see the [Ray Compute Engine documentation](../compute-engine/ray.md#configuration-options).

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

# Azure Blob Storage / Azure Data Lake Storage Gen2
az_storage = SavedDatasetFileStorage(path="abfss://container@stc_account.dfs.core.windows.net/datasets/driver_features.parquet")
job.persist(az_storage, allow_overwrite=True)
```

### Using Ray Cluster

#### Standard Ray Cluster

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
    storage_path: s3://my-bucket/features
```

3. For multiple worker nodes:
```bash
# On worker nodes
ray start --address='head-node-ip:10001'
```

#### KubeRay Cluster (Kubernetes)

To use Feast with Ray clusters on Kubernetes via CodeFlare SDK:

**Prerequisites:**
- KubeRay cluster deployed on Kubernetes
- CodeFlare SDK installed: `pip install codeflare-sdk`
- Access credentials for the Kubernetes cluster

**Configuration:**

1. Using configuration file:
```yaml
offline_store:
    type: ray
    use_kuberay: true
    storage_path: s3://my-bucket/feast-data
    kuberay_conf:
        cluster_name: "feast-ray-cluster"
        namespace: "feast-system"
        auth_token: "${RAY_AUTH_TOKEN}"
        auth_server: "https://api.openshift.com:6443"
        skip_tls: false
    enable_ray_logging: false
```

2. Using environment variables:
```bash
export FEAST_RAY_USE_KUBERAY=true
export FEAST_RAY_CLUSTER_NAME=feast-ray-cluster
export FEAST_RAY_AUTH_TOKEN=your-k8s-token
export FEAST_RAY_AUTH_SERVER=https://api.openshift.com:6443
export FEAST_RAY_NAMESPACE=feast-system
export FEAST_RAY_SKIP_TLS=false

# Then use standard Feast code
python your_feast_script.py
```

**Features:**
- The CodeFlare SDK handles cluster connection and authentication
- Automatic TLS certificate management
- Authentication with Kubernetes clusters
- Namespace isolation
- Secure communication between client and Ray cluster
- Automatic cluster discovery

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
4. **No Complex Transformations**: The Ray offline store focuses on data I/O operations. For complex feature transformations (aggregations, joins, custom UDFs), use the [Ray Compute Engine](../compute-engine/ray.md) instead

## Integration with Ray Compute Engine

For complex feature processing operations, use the Ray offline store in combination with the [Ray Compute Engine](../compute-engine/ray.md). See the **Ray Offline Store + Compute Engine** configuration example in the [Configuration](#configuration) section above for a complete setup.


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

# Offline write batch (materialization)
# Create sample data for materialization
data = pa.table({
    "driver_id": [1, 2, 3, 4, 5],
    "avg_daily_trips": [10.5, 15.2, 8.7, 12.3, 9.8],
    "event_timestamp": [datetime.now()] * 5
})

# Write batch to offline store
RayOfflineStore.offline_write_batch(
    config=store.config,
    feature_view=driver_stats_fv,
    table=data,
    progress=lambda rows: print(f"Processed {rows} rows")
)
```

For complete examples, see the [Configuration](#configuration) section above.