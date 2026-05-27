---
title: "Scalable Embedding Pipelines with Feast + Spark: Pre-Computed Features for Training and Serving"
description: "Learn how Feast's BatchFeatureView and SparkComputeEngine work together to build embedding pipelines that materialize once, serve online, and retrieve pre-computed features at training time — without re-running expensive UDFs."
date: 2026-05-27
authors: ["abhijeet-dhumal"]
---

<div class="hero-image">
  <img src="/images/blog/feast_spark_embedding_pipeline.png" alt="Feast + Spark Embedding Pipeline Architecture" loading="lazy">
</div>

Embedding features are the backbone of modern AI applications — from semantic search and RAG pipelines to recommendation engines and anomaly detection. But teams that run these pipelines on Spark face a painful reality: **the embedding UDF is expensive**, and every time you call `get_historical_features()` for a training job, you're paying that cost again.

This post walks through how Feast's `BatchFeatureView` with `SparkComputeEngine` now lets you **materialize once, serve online, and read pre-computed features at training time** — without re-running UDFs, without manual export scripts, and without duplicating your transformation logic.

## The Problem: UDFs Are Expensive, and Training Jobs Pay Twice

A typical embedding pipeline on Spark looks like this:

```python
@batch_feature_view(
    sources=[SparkSource(query="SELECT id, text FROM bronze.documents")],
    schema=[Field(name="embedding", dtype=Array(Float32), vector_index=True)],
    online=True,
)
def document_embeddings(df: DataFrame) -> DataFrame:
    # Load a 400MB model per executor, run inference on millions of rows
    embeddings = spark_embed(df, "all-MiniLM-L6-v2", input_col="text")
    return embeddings
```

`feast materialize` runs this UDF and pushes embeddings to your online store (Milvus, Redis). Great for serving.

Then a data scientist calls `get_historical_features()` to build a training set. Feast re-runs the UDF from scratch — same raw query, same model load, same inference. **For a 10M-row document corpus, this means 20–40 minutes of compute on every training run.**

The fix should be simple: after materialization, the features already exist as parquet in your data lake. Training jobs should read from there, not re-execute the UDF.

## The Solution: `SparkSource` with `query + path`

Feast now supports a `SparkSource` that holds both a `query` (for raw data ingestion) and a `path` (for pre-computed offline write-back and read):

```python
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource

source = SparkSource(
    query="SELECT id, text, event_timestamp FROM bronze.documents",
    path="s3://my-bucket/feast/features/document_embeddings/",
    file_format="parquet",
    timestamp_field="event_timestamp",
)
```

The semantics are clear:

- **`query`** — used during `materialize()` to read raw data and pass it through the UDF
- **`path`** — used as the write-back destination (`offline=True`) and as the pre-computed read source in `get_historical_features()`

With this, Feast routes requests correctly without any extra configuration:

| Operation | What Feast reads |
|-----------|-----------------|
| `feast materialize` | `query` → UDF → write to `path` + online store |
| `get_historical_features()` | `path` (pre-computed parquet), no UDF |

## Defining the Full Pipeline

Here's a complete `BatchFeatureView` definition for a document embedding pipeline:

```python
from feast import BatchFeatureView, Entity, Field, FeatureStore
from feast.types import Array, Float32, Int64, String
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast.infra.compute_engines.spark.utils import spark_embed
from datetime import timedelta

document = Entity(name="document_id", join_keys=["document_id"])

source = SparkSource(
    query="SELECT document_id, title, body, event_timestamp FROM bronze.documents",
    path="s3://my-bucket/feast/features/document_embeddings/",
    file_format="parquet",
    timestamp_field="event_timestamp",
)

@batch_feature_view(
    entities=[document],
    sources=[source],
    schema=[
        Field(name="document_id", dtype=Int64),
        Field(name="embedding", dtype=Array(Float32), vector_index=True),
        Field(name="title", dtype=String),
    ],
    online=True,
    offline=True,      # write pre-computed features to source.path
    ttl=timedelta(days=90),
)
def document_embeddings(df):
    """
    Generates sentence embeddings from document text.
    Runs once during materialize; get_historical_features reads from path.
    """
    return spark_embed(df, model="all-MiniLM-L6-v2", input_col="body")
```

### Materialization: Run Once

```bash
feast apply
feast materialize 2024-01-01T00:00:00 2026-05-27T00:00:00
```

Feast executes the UDF via `SparkComputeEngine`, writes results to both:

1. Your online store (`Milvus` / `Redis`) for real-time vector search
2. `s3://my-bucket/feast/features/document_embeddings/` for offline training data

### Training: Read Pre-Computed Features

```python
import pandas as pd
from feast import FeatureStore

store = FeatureStore(repo_path=".")

entity_df = pd.DataFrame({
    "document_id": [101, 202, 303],
    "event_timestamp": pd.to_datetime(["2026-05-01", "2026-05-01", "2026-05-01"]),
})

# Reads directly from s3://my-bucket/.../document_embeddings/ — no UDF re-execution
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=["document_embeddings:embedding", "document_embeddings:title"],
).to_df()
```

Training time drops from 20–40 minutes to the time it takes to read parquet. The UDF only runs during `materialize`.

### Online Serving: Vector Search

```python
query_embedding = model.encode(["machine learning infrastructure"])[0].tolist()

results = store.retrieve_online_documents_v2(
    features=["document_embeddings:embedding", "document_embeddings:title"],
    query=query_embedding,
    top_k=10,
).to_dict()
```

## How It Works Under the Hood

### Fix 1: `foreachPartition` Replaces `mapInArrow`

Spark 3.5 introduced `WindowGroupLimitExec`, which is inserted upstream of `MapInArrowExec` when UDFs involve Window operations. This routes the Python worker through the wrong serializer, producing:

```
AttributeError: 'list' object has no attribute 'dtype'
```

The fix replaces `mapInArrow` with `foreachPartition`, which uses pickle serialization instead of Arrow. The Arrow UDF bridge mismatch cannot occur:

```python
# Before (broken on Spark 3.5+ with Window ops)
df.mapInArrow(write_partition, schema)

# After (works on all Spark versions)
rdd.foreachPartition(write_partition_chunked)
```

`foreachPartition` also gives us direct control over write chunking, which solves a second issue on large feature views.

### Fix 2: Chunked Writes Prevent OOMKill

On large feature views (10M+ rows), loading an entire partition into Python memory before writing caused `MemoryError` on executor pods. Writes are now chunked:

```python
def write_partition_chunked(rows, chunk_size=1000):
    buffer = []
    for row in rows:
        buffer.append(row)
        if len(buffer) >= chunk_size:
            online_store.online_write_batch(config, table, buffer, ...)
            buffer.clear()
    if buffer:
        online_store.online_write_batch(config, table, buffer, ...)
```

This bounds Python memory per executor to `chunk_size × row_size` regardless of partition size.

### Fix 3: S3/GCS PyArrow Filesystem

When Feast reads staging parquet back from S3 (for point-in-time joins after materialization), the old implementation passed raw `s3://` URIs to `pyarrow.dataset`. This works on HDFS but fails on S3-compatible stores (MinIO, Ceph, AWS with custom endpoints):

```python
# Now resolves the correct PyArrow filesystem from the URI scheme
fs, stripped_paths = self._resolve_staging_filesystem(parquet_paths)
dataset = ds.dataset(stripped_paths, format="parquet", filesystem=fs)
```

The resolver picks up `AWS_ENDPOINT_URL_S3` for MinIO/LocalStack, `AWS_DEFAULT_REGION`, and builds a `pafs.S3FileSystem` or `pafs.GcsFileSystem` accordingly — enabling on-prem and private cloud deployments.

## Feature Store Configuration

```yaml
# feature_store.yaml
project: my_project
provider: local
registry: s3://my-bucket/feast/registry.db

offline_store:
  type: spark
  spark_conf:
    spark.master: "k8s://https://my-cluster:6443"
    spark.executor.instances: "4"
    spark.executor.memory: "8g"
    spark.executor.resource.gpu.amount: "1"   # optional GPU acceleration
    spark.kubernetes.container.image: "my-registry/feast-spark:latest"

online_store:
  type: milvus
  path: data/online_store.db
  vector_enabled: true
  embedding_dim: 384
  index_type: "IVF_FLAT"
  distance_metric: "COSINE"

compute_engine:
  type: spark
```

## Graceful Fallback for First Runs

Before the first `materialize`, `source.path` doesn't exist yet. Feast now handles this gracefully — instead of crashing, it falls back to executing the query live:

```
[INFO] Offline path s3://my-bucket/.../document_embeddings/ not yet populated.
       Falling back to live query for get_historical_features.
```

After the first successful materialization, subsequent `get_historical_features()` calls automatically switch to reading from `path`.

## Summary

| Capability | Before | After |
|-----------|--------|-------|
| `get_historical_features()` for BFVs | Re-runs UDF every call | Reads pre-computed parquet |
| Spark 3.5+ materialization | Fails with Arrow UDF error | Works via `foreachPartition` |
| Large feature views (10M+ rows) | OOMKill on executor | Chunked writes, bounded memory |
| S3/MinIO staging reads | Fails on custom endpoints | Resolves `S3FileSystem` from env |
| First run (no offline path yet) | Crashes | Graceful fallback to live query |

These improvements are available as of Feast 0.64. The fixes apply to any `BatchFeatureView` using `SparkComputeEngine`, and the `query + path` SparkSource pattern works with all Feast-supported online stores.

## Getting Started

```bash
pip install "feast[spark]>=0.64"
```

Check out the [Spark compute engine docs](https://docs.feast.dev/reference/compute-engine/spark) and the [BatchFeatureView reference](https://docs.feast.dev/reference/feature-view-types/batch-feature-view) for configuration details.

If you're building embedding pipelines on Spark and want to share your experience, join us on [Slack](https://slack.feast.dev) or open a discussion on [GitHub](https://github.com/feast-dev/feast).
