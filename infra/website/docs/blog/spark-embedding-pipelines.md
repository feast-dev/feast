---
title: "BatchFeatureView + Spark on Kubernetes: From Broken to Production-Ready"
description: "A practical guide to the six fixes that made BatchFeatureView with SparkComputeEngine reliable on Kubernetes — covering silent failures, Spark 3.5 serializer bugs, S3A event logging, MinIO deployments, and efficient training data retrieval."
date: 2026-05-27
authors: ["abhijeet-dhumal"]
---

<div class="hero-image">
  <img src="/images/blog/feast_spark_k8s_pipeline.png" alt="Feast BatchFeatureView on Kubernetes with Spark" loading="lazy">
</div>

`BatchFeatureView` with `SparkComputeEngine` is Feast's answer to large-scale feature engineering — run arbitrary Spark transformations at materialization time, write features to both an online store and an offline parquet path, and retrieve pre-computed features for training without re-running the UDF. On paper, it's exactly what ML platform teams need for production pipelines on Kubernetes.

In practice, it didn't work.

This post documents the six bugs we hit while building a real recommendation system on OpenShift AI with PySpark 3.5 and MinIO, the fixes we contributed back to Feast, and the architecture that's now running in production.

## The Pipeline We Were Building

The smartshop-ai demo is a product recommendation system on Amazon Reviews data. The feature pipeline has three layers:

```
Raw S3 Parquet (reviews + metadata)
    │  SparkComputeEngine (K8s GPU executors)
    ▼
@batch_feature_view transformations
    ├── user_features  → Redis  (user aggregates, online serving)
    ├── item_features  → Redis  (item aggregates, online serving)
    ├── item_metadata  → Redis  (catalog fields, online serving)
    ├── interactions   → S3     (user-item labels, training only)
    └── review_embeddings → Milvus (384-dim vectors, semantic search)
```

All five are `@batch_feature_view` with `TransformationMode.PYTHON` and `SparkComputeEngine`. The tabular BFVs use a `query + path` `SparkSource` so materialization writes to both Redis and S3 offline parquet — and `get_historical_features()` reads from the pre-computed parquet instead of re-running the UDF.

Getting this to work required fixing six distinct failure modes, shipped as six PRs.

---

## Failure 1: Every Default BFV Was Broken Out of the Box

**PR:** [#6310](https://github.com/feast-dev/feast/pull/6310) — `fix(spark): BatchFeatureView with TransformationMode.PYTHON now reads all source columns`

The first materialization attempt failed with a cascade of `AnalysisException`:

```
AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION]
A column or function parameter with name 'user_avg_rating' cannot be resolved.
```

`TransformationMode.PYTHON` is the **default mode** for `@batch_feature_view`. The bug: `FeatureBuilder.get_column_info()` set `feature_cols=[]` (the "read all source columns" signal) only for `ray` and `pandas` modes — `python` was missing.

With `feature_cols` populated from the BFV's *output* schema (`["user_avg_rating", "user_review_count", ...]`), Feast tried to SELECT those column names from the raw source. Those columns don't exist in raw data — the UDF is supposed to **compute** them.

**Fix:** Add `"python"` to the mode guard in `get_column_info()`. One line, but it unblocked every default `@batch_feature_view` on Spark.

---

## Failure 2: Silent NULL Output Even After the Schema Fix

**PR:** [#6311](https://github.com/feast-dev/feast/pull/6311) — `fix: Use SELECT * when feature_name_columns is empty in pull_all_from_table_or_query`

With `feature_cols=[]` now correctly set for python mode, the code was supposed to SELECT all source columns. It didn't. `pull_all_from_table_or_query` always built an explicit SELECT from `join_key_columns + feature_name_columns + timestamp_fields`. With `feature_name_columns=[]`, that produced:

```sql
SELECT user_id, event_timestamp
FROM s3a://bucket/raw/reviews/
WHERE event_timestamp BETWEEN ...
```

The UDF received a 2-column DataFrame. Every aggregation silently returned `null`. Features wrote to Redis — all nulls. No error, exit code 0.

**Fix:** When `feature_name_columns=[]`, emit `SELECT *` instead of an explicit column list:

```python
if not feature_name_columns:
    fields_with_alias_string = "*"
else:
    fields_with_aliases, _ = _get_fields_with_aliases(...)
    fields_with_alias_string = ", ".join(fields_with_aliases)
```

After these two fixes, the UDFs ran correctly and produced real feature values.

---

## Failure 3: SparkContext Crashes Silently When Event Logging Points to S3A

**PR:** [#6317](https://github.com/feast-dev/feast/pull/6317) — `fix: Pre-create S3A event log dir before SparkContext init`

With Spark event logging enabled for debugging (a reasonable production setting):

```yaml
spark.eventLog.enabled: "true"
spark.eventLog.dir: "s3a://my-bucket/spark-logs/"
```

`feast materialize` exited with code 0 and wrote nothing. The failure chain:

```
SparkContext.__init__
  └─ EventLogFileWriter.requireLogBaseDirAsDirectory()
       └─ S3A 404 (prefix doesn't exist) → raises RuntimeException
            └─ caught by _materialize_one(except Exception) → ERROR job
                 └─ CLI exits 0 — no data written, no visible error
```

S3 has no real directories. A fresh bucket looks identical to a non-existent prefix, so Spark's pre-flight check always fails on first use.

**Fix:** Before building the `SparkSession`, call `_ensure_s3a_event_log_dir()` which writes a zero-byte `.keep` placeholder if the prefix is empty. Uses boto3 (already a Feast dependency), reads credentials from Spark config first with env var fallback, and supports MinIO path-style addressing:

```python
addressing_style = (
    "path"
    if spark_config.get("spark.hadoop.fs.s3a.path.style.access", "false") == "true"
    else "auto"
)
```

Non-fatal: logs a warning and lets Spark surface its own error if the write fails.

---

## Failure 4: Materialization Fails on Spark 3.5+ With Window Operations

**PR:** [#6441](https://github.com/feast-dev/feast/pull/6441) — `fix(spark): Replace mapInArrow with foreachPartition and bound write memory`

The `user_features` BFV uses `Window.partitionBy` to compute the user's primary category:

```python
w = Window.partitionBy("user_id").orderBy(F.desc("cat_count"))
user_primary = user_cat_counts.withColumn("rn", F.row_number().over(w))...
```

On Spark 3.5 (Databricks Runtime 14+, EMR 7+), this triggered a serializer mismatch:

```
AttributeError: 'list' object has no attribute 'dtype'
```

Spark 3.5 introduced `WindowGroupLimitExec`, which inserts itself upstream of `MapInArrowExec` and routes the Python worker through the wrong serializer. The fix replaces `mapInArrow` (Arrow UDF bridge) with `foreachPartition` (pickle-based), which cannot hit this mismatch.

The same PR adds chunked writes (`chunk_size=1000`) so large feature views (26M+ keys in our Redis deployment) don't cause `MemoryError` on executor pods:

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

Also re-applies `spark.sql.*` / `spark.hadoop.*` session configs after `SparkSession.getOrCreate()`, which silently drops overrides when reusing a warm K8s session.

---

## Failure 5: Training Jobs Re-Run the Full UDF Every Time

**PR:** [#6440](https://github.com/feast-dev/feast/pull/6440) — `feat(spark): SparkSource query+path and pre-computed offline read for BatchFeatureView`

Once materialization worked reliably, the next problem: every call to `get_historical_features()` re-ran the full UDF on raw data. For `user_features` on 26M review rows, that's 15–20 minutes of Spark compute per training run — even though the features already existed in S3 from the last `materialize`.

**The pattern:** `SparkSource` now accepts `query + path` together:

```python
user_reviews_source = SparkSource(
    name="user_reviews_source",
    query=(
        "SELECT *, CAST(timestamp / 1000 AS TIMESTAMP) AS event_timestamp "
        "FROM parquet.`s3a://smartshop-raw/raw/reviews/*/`"
    ),
    path="s3a://smartshop-features/offline/user_features/",
    file_format="parquet",
    timestamp_field="event_timestamp",
)
```

| Field | Used during |
|-------|------------|
| `query` | `feast materialize` → raw data → UDF → write to `path` + online store |
| `path` | `get_historical_features()` → reads pre-computed parquet directly |

Materialization writes to both Redis and `s3a://smartshop-features/offline/user_features/`. Training reads directly from that path — no UDF, no raw data scan:

```python
# Training: reads pre-computed parquet, not raw reviews
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "user_features:user_avg_rating",
        "user_features:user_review_count",
        "item_features:item_avg_rating",
        "interactions:label",
    ],
).to_df()
```

The `interactions` BFV (`online=False, offline=True`) is the offline-only training label view — too large for Redis, needed purely for training. This PR also fixes the `feature_store.py` validation that incorrectly rejected `online=False` BFVs during retrieval.

Graceful fallback: if `path` doesn't exist yet (before first `materialize`), Feast falls back to the live query automatically.

---

## Failure 6: Staging Reads Fail on MinIO and Private Cloud S3

**PR:** [#6442](https://github.com/feast-dev/feast/pull/6442) — `fix(spark): S3/GCS PyArrow filesystem resolution for staging paths`

After `materialize`, Feast reads staging parquet back via PyArrow for point-in-time joins. The old code passed raw `s3://` / `s3a://` URIs directly to `pyarrow.dataset`:

```
FileNotFoundError: s3://my-bucket/...
```

This works on AWS with default credentials but fails on MinIO, LocalStack, or any S3-compatible store with a custom endpoint. The fix builds a proper `pyarrow.fs.S3FileSystem` from the URI scheme:

```python
def _resolve_staging_filesystem(self, paths):
    sample = paths[0]
    if sample.startswith("s3://") or sample.startswith("s3a://"):
        endpoint = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_S3_ENDPOINT")
        region = getattr(self._config.offline_store, "region", None) or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
        kwargs = {"region": region}
        if endpoint:
            kwargs["endpoint_override"] = endpoint.rstrip("/").replace("https://", "").replace("http://", "")
            kwargs["scheme"] = "https" if endpoint.startswith("https") else "http"
        fs = pafs.S3FileSystem(**kwargs)
        stripped = [p.replace("s3a://", "").replace("s3://", "") for p in paths]
        return fs, stripped
    # GCS and local paths handled similarly
```

Picks up `AWS_ENDPOINT_URL_S3` (the standard env var for MinIO/LocalStack) automatically. No configuration changes needed.

---

## The Full Working Architecture

With all six fixes in place, the pipeline runs end-to-end on OpenShift AI:

```
                    feast materialize
                          │
              ┌───────────┼───────────────────┐
              ▼           ▼                   ▼
        user_features  item_features    review_embeddings
        item_metadata  interactions
              │           │                   │
        SparkComputeEngine (K8s GPU pods, Spark 3.5)
              │           │                   │
         Redis (online)  S3 offline        Milvus (vector search)
              │           │
              └─────┬─────┘
                    │
           get_historical_features()
           (reads S3, no UDF re-run)
                    │
              training_df → model training
```

```yaml
# feature_store.yaml
offline_store:
  type: spark
  spark_conf:
    spark.master: "k8s://https://openshift-api:6443"
    spark.executor.instances: "4"
    spark.executor.resource.gpu.amount: "1"
    spark.eventLog.enabled: "true"
    spark.eventLog.dir: "s3a://smartshop-logs/spark-events/"
    spark.hadoop.fs.s3a.endpoint: "http://minio.feast-demo.svc:9000"
    spark.hadoop.fs.s3a.path.style.access: "true"

online_store:
  type: redis
  connection_string: "redis://redis.feast-demo.svc:6379"
```

**Numbers from our deployment:**
- 26M+ feature vectors materialized to Redis across 4 GPU executor pods
- `get_historical_features()` for a 500K entity training set: **45 seconds** (reading S3 parquet) vs **18 minutes** (re-running UDF)
- Review embeddings (384-dim, `all-MiniLM-L6-v2`) → Milvus: ~8M vectors

---

## Summary of Fixes

| PR | What was broken | Impact |
|----|----------------|--------|
| [#6310](https://github.com/feast-dev/feast/pull/6310) | `TransformationMode.PYTHON` BFVs tried to SELECT output column names from raw source | Every default BFV on Spark failed |
| [#6311](https://github.com/feast-dev/feast/pull/6311) | `feature_name_columns=[]` generated broken SELECT, UDF got empty DataFrame | Silent null features, no error |
| [#6317](https://github.com/feast-dev/feast/pull/6317) | S3A event log dir not pre-created, SparkContext crash swallowed silently | Silent 0-byte materialization |
| [#6441](https://github.com/feast-dev/feast/pull/6441) | Spark 3.5 Window+Arrow serializer mismatch; OOM on large partitions | Materialization crash on modern Spark |
| [#6440](https://github.com/feast-dev/feast/pull/6440) | `get_historical_features()` re-ran full UDF every call | 18 min per training run |
| [#6442](https://github.com/feast-dev/feast/pull/6442) | PyArrow staging reads failed on MinIO/custom S3 endpoints | On-prem/private cloud deployments broken |

All six fixes are available in Feast 0.64+.

---

If you're running `BatchFeatureView` with `SparkComputeEngine` on Kubernetes, especially with MinIO or a custom S3-compatible store, these changes make the difference between a pipeline that silently produces nothing and one that reliably materializes at scale. Join the conversation on [Slack](https://slack.feast.dev) or [GitHub](https://github.com/feast-dev/feast).
