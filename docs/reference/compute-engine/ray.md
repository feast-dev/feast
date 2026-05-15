# Ray Compute Engine (contrib)

The Ray compute engine is a distributed compute implementation that leverages [Ray](https://www.ray.io/) for executing feature pipelines including transformations, aggregations, joins, and materializations. It provides scalable and efficient distributed processing for both `materialize()` and `get_historical_features()` operations.

## Quick Start with Ray Template

### Ray RAG Template - Batch Embedding at Scale

For RAG (Retrieval-Augmented Generation) applications with distributed embedding generation:

```bash
feast init -t ray_rag my_rag_project
cd my_rag_project/feature_repo
```

The Ray RAG template demonstrates:
- **Parallel Embedding Generation**: Uses Ray compute engine to generate embeddings across multiple workers
- **Vector Search Integration**: Works with Milvus for semantic similarity search
- **Complete RAG Pipeline**: Data → Embeddings → Search workflow

The Ray compute engine automatically distributes the embedding generation across available workers, making it ideal for processing large datasets efficiently.

## Overview

The Ray compute engine provides:
- **Distributed DAG Execution**: Executes feature computation DAGs across Ray clusters
- **Intelligent Join Strategies**: Automatic selection between broadcast and distributed joins
- **Lazy Evaluation**: Deferred execution for optimal performance
- **Resource Management**: Automatic scaling and resource optimization
- **Point-in-Time Joins**: Efficient temporal joins for historical feature retrieval
- **GPU Support**: Schedule transformation workers on GPU nodes via `num_gpus` config (all modes including KubeRay)

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
    max_workers: 4                         # Optional: Maximum number of workers
    enable_optimization: true              # Optional: Enable performance optimizations
    broadcast_join_threshold_mb: 100       # Optional: Broadcast join threshold (MB)
    max_parallelism_multiplier: 2          # Optional: Parallelism multiplier
    target_partition_size_mb: 64           # Optional: Target partition size (MB)
    window_size_for_joins: "1H"            # Optional: Time window for distributed joins
    ray_address: localhost:10001           # Optional: Ray cluster address
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `type` | string | `"ray.engine"` | Must be `ray.engine` |
| `max_workers` | int | None (uses all cores) | Maximum number of Ray workers |
| `enable_optimization` | boolean | true | Enable performance optimizations |
| `broadcast_join_threshold_mb` | int | 100 | Size threshold for broadcast joins (MB) |
| `max_parallelism_multiplier` | int | 2 | Parallelism as multiple of CPU cores |
| `target_partition_size_mb` | int | 64 | Target partition size (MB) |
| `window_size_for_joins` | string | "1H" | Time window for distributed joins |
| `ray_address` | string | None | Ray cluster address (triggers REMOTE mode) |
| `use_kuberay` | boolean | None | Enable KubeRay mode (overrides ray_address) |
| `kuberay_conf` | dict | None | **KubeRay configuration dict** with keys: `cluster_name` (required), `namespace` (default: "default"), `auth_token`, `auth_server`, `skip_tls` (default: false) |
| `enable_ray_logging` | boolean | false | Enable Ray progress bars and logging |
| `enable_distributed_joins` | boolean | true | Enable distributed joins for large datasets |
| `staging_location` | string | None | Remote path for batch materialization jobs |
| `ray_conf` | dict | None | Ray configuration parameters (memory, CPU limits) |
| `num_gpus` | float | None | Number of GPUs to request per worker task. Requires GPU nodes in the Ray cluster. Fractional values (e.g. `0.5`) are supported. Supported in all modes including KubeRay. |
| `gpu_batch_format` | string | `"pandas"` | Batch format for `map_batches` when `num_gpus` is set. Use `"numpy"` or `"pyarrow"` for GPU-native libraries (e.g. cuDF, PyTorch). |
| `worker_task_options` | dict | None | Arbitrary Ray `.options()` kwargs applied to every worker task. See [Worker Resource Scheduling](#worker-resource-scheduling) for the full reference. |

### Mode Detection Precedence

The Ray compute engine automatically detects the execution mode:

1. **Environment Variables** → KubeRay mode (if `FEAST_RAY_USE_KUBERAY=true`)
2. **Config `kuberay_conf`** → KubeRay mode
3. **Config `ray_address`** → Remote mode
4. **Default** → Local mode

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
- Automatically selected when feature data < 100MB
- Features are cached in Ray's object store
- Entities are distributed across cluster
- Each worker gets a copy of feature data

### Distributed Windowed Join

Used for large feature datasets:
- Automatically selected when feature data > 100MB
- Data is partitioned by time windows
- Point-in-time joins within each window
- Results are combined across windows

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
print(f"Available GPUs: {resources.get('GPU', 0)}")
print(f"Available memory: {resources.get('memory', 0) / 1e9:.2f} GB")

# Monitor job progress
job = store.get_historical_features(...)
# Ray compute engine provides built-in progress tracking
```

## Worker Resource Scheduling

`worker_task_options` is a passthrough dict of [Ray `.options()` kwargs](https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote_function.RemoteFunction.options.html) applied to every worker task Feast dispatches. It pairs with `ray_conf` (cluster-level `ray.init` options) — `worker_task_options` targets individual worker tasks. Options are forwarded at two levels so Ray schedules correctly:

1. On the `@ray.remote` orchestration task via `.options(**worker_task_options)` — controls node selection.
2. Inside `map_batches` for the scheduling-relevant subset (`num_gpus`, `num_cpus`, `accelerator_type`, `resources`) — controls which nodes run the data workers.

This is supported across **all execution modes**: local, remote, and KubeRay.

### Common `worker_task_options` keys

| Key | Type | Description |
|-----|------|-------------|
| `num_cpus` | float | CPUs per task (default: 1). Fractional values supported. |
| `memory` | int | Heap memory in **bytes** (e.g. `8589934592` for 8 GB). |
| `accelerator_type` | string | Pin tasks to a specific GPU model — `"A100"`, `"T4"`, `"V100"`, etc. Useful on KubeRay clusters with mixed GPU node pools. |
| `resources` | dict | Custom/Kubernetes extended resource labels, e.g. `{"intel.com/gpu": 1}`. |
| `runtime_env` | dict | Per-task [Ray runtime environment](https://docs.ray.io/en/latest/ray-core/handling-dependencies.html) — `pip`, `conda`, `env_vars`, `working_dir`, etc. For KubeRay, use this to install packages on worker pods without rebuilding images. |
| `max_retries` | int | Task retry count on worker failure (default: 3). |
| `scheduling_strategy` | string | `"DEFAULT"`, `"SPREAD"`, or a placement group strategy. |

> For the full list of supported keys see the [Ray RemoteFunction.options() API docs](https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote_function.RemoteFunction.options.html).

### GPU support

`num_gpus` is the only first-class GPU field because it also drives `gpu_batch_format` selection inside Feast. Set it directly rather than inside `worker_task_options`:

```yaml
batch_engine:
    type: ray.engine
    num_gpus: 1               # GPUs per task (fractional values like 0.5 supported)
    gpu_batch_format: numpy   # numpy/pyarrow for GPU-native libs (cuDF, PyTorch)
```

When `num_gpus` is set your transformation UDF runs on a GPU worker:

```python
import cudf  # RAPIDS cuDF – GPU-accelerated DataFrame library

def gpu_transform(batch):
    gpu_df = cudf.from_pandas(batch)
    gpu_df["score"] = gpu_df["raw_value"] * 2.0
    return gpu_df.to_pandas()
```

### Full example — KubeRay with GPU + all common options

```yaml
batch_engine:
    type: ray.engine
    use_kuberay: true
    kuberay_conf:
        cluster_name: "feast-gpu-cluster"
        namespace: "feast-system"
        auth_token: "${RAY_AUTH_TOKEN}"
        auth_server: "https://api.openshift.com:6443"
    num_gpus: 1
    gpu_batch_format: numpy
    worker_task_options:
        num_cpus: 4
        memory: 8589934592        # 8 GB
        accelerator_type: "A100"  # pin to A100 nodes on mixed GPU pool
        max_retries: 5
        runtime_env:
            pip:
                - cudf-cu12==24.10.0
                - torch==2.4.0
            env_vars:
                CUDA_VISIBLE_DEVICES: "0"
```

### Checking cluster resources

```python
import ray

ray.init(address="auto")
resources = ray.cluster_resources()
print(f"Available CPUs: {resources.get('CPU', 0)}")
print(f"Available GPUs: {resources.get('GPU', 0)}")
print(f"Available memory: {resources.get('memory', 0) / 1e9:.2f} GB")
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
    ray_address: "ray://ray-cluster:10001"
    broadcast_join_threshold_mb: 50
```

### With Feature Transformations

#### On-Demand Transformations

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

#### Ray Native Transformations

For distributed transformations that leverage Ray's dataset and parallel processing capabilities, use `mode="ray"` in your `BatchFeatureView`:

```python
# Feature view with Ray transformation mode
document_embeddings_view = BatchFeatureView(
    name="document_embeddings",
    entities=[document],
    mode="ray",  # Enable Ray native transformation
    ttl=timedelta(days=365),
    schema=[
        Field(name="document_id", dtype=String),
        Field(name="embedding", dtype=Array(Float32), vector_index=True),
        Field(name="movie_name", dtype=String),
        Field(name="movie_director", dtype=String),
    ],
    source=movies_source,
    udf=generate_embeddings_ray_native,
    online=True,
)
```

For more information, see the [Ray documentation](https://docs.ray.io/en/latest/) and [Ray Data guide](https://docs.ray.io/en/latest/data/getting-started.html). 