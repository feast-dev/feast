# Using MLflow as a Data Source in Feast

This guide walks through the end-to-end workflow for using MLflow as a **first-class offline DataSource** in Feast. You define `FeatureView` objects backed by `MlflowDatasetSource`, then use `get_historical_features()` and `create_saved_dataset()` without manual export or ETL.

For API reference, see [MLflow Integration](../reference/mlflow.md#mlflow-as-a-datasource). For operator auto-discovery of MLflow on RHOAI, see [Guide 8 — MLflow Integration](feast-operator/08-mlflow-integration.md).

## Overview

| Mode | Use case | Configuration |
|------|----------|---------------|
| **GenAI Dataset** | Curated evaluation / trace records from MLflow GenAI Datasets | `dataset_name` or `dataset_id` |
| **Artifact** | Parquet or CSV artifacts logged on MLflow runs | `run_id`, `artifact_path`, `artifact_format` |

**Scope (Tech Preview):** offline/historical retrieval only; Parquet and CSV tabular artifacts; reads go through the MLflow Tracking API (no direct object-storage bypass).

## Prerequisites

- Feast with MLflow support: `pip install 'feast[mlflow]'` (MLflow ≥ 2.10 for GenAI datasets).
- A reachable MLflow tracking server.
- On RHOAI: Feast and MLflow operators deployed; FeatureStore CR in Ready state; cluster CA bundle available for TLS if using HTTPS.

## Step 1: Configure `feature_store.yaml`

```yaml
project: my_project
registry: data/registry.db
provider: local
offline_store: duckdb
online_store:
  type: sqlite
  path: data/online_store.db
entity_key_serialization_version: 3

mlflow:
  enabled: true
  tracking_uri: https://mlflow.example.com:8443
  ca_bundle: /etc/pki/tls/certs/ca-bundle.crt
  supported_artifact_formats:
    - parquet
    - csv
  request_timeout: 30        # seconds per HTTP request to MLflow (default: 30)
  max_retries: 3             # retry attempts for transient failures (default: 3)
  retry_backoff_factor: 1.0  # exponential backoff base (default: 1.0 → 1s, 2s, 4s)
```

On operator-managed clusters, `tracking_uri` and `ui_url` are often injected automatically when an `MLflow` CR is present. You can still set `ca_bundle` explicitly for DataSource reads from a workbench or CI job.

| Field | Default | Description |
|---|---|---|
| `tracking_uri` | `MLFLOW_TRACKING_URI` env | MLflow tracking server URI |
| `ca_bundle` | `None` | CA bundle path for TLS (falls back to `REQUESTS_CA_BUNDLE` env) |
| `supported_artifact_formats` | `["parquet", "csv"]` | Accepted artifact formats |
| `request_timeout` | `30` | Seconds per HTTP request to MLflow |
| `max_retries` | `3` | Retry attempts for transient failures (timeouts, 5xx) |
| `retry_backoff_factor` | `1.0` | Exponential backoff base (wait = factor × 2^attempt) |

## Step 2: Define `MlflowDatasetSource` FeatureViews

`MlflowDatasetSource` is read-only. You must set a `batch_source` (for example `FileSource`) for writeback paths such as `create_saved_dataset()` and materialization.

### GenAI Dataset mode

```python
from feast import Entity, FeatureView, Field, FileSource
from feast.infra.data_sources.mlflow import MlflowDatasetSource
from feast.types import String

batch_sink = FileSource(
    path="data/eval_records.parquet",
    timestamp_field="event_timestamp",
)

source = MlflowDatasetSource(
    name="eval_dataset",
    dataset_name="production_validation_set",
    batch_source=batch_sink,
    timestamp_field="event_timestamp",
    field_mapping={"expectations.expected_response": "expected_response"},
)

eval_records = FeatureView(
    name="eval_records",
    entities=[Entity(name="record_id", join_keys=["record_id"])],
    schema=[Field(name="expected_response", dtype=String)],
    source=source,
)
```

### Artifact mode (Parquet)

```python
source = MlflowDatasetSource(
    name="training_features",
    run_id="<mlflow-run-id>",
    artifact_path="outputs/features.parquet",
    artifact_format="parquet",
    batch_source=FileSource(
        path="data/features.parquet",
        timestamp_field="event_timestamp",
    ),
    timestamp_field="event_timestamp",
)

training_features = FeatureView(
    name="training_features",
    entities=[Entity(name="record_id", join_keys=["record_id"])],
    schema=[...],
    source=source,
)
```

## Step 3: Apply and validate

```bash
feast apply
feast mlflow list-sources
feast mlflow validate-source eval_records
```

`validate-source` checks configuration, MLflow reachability, schema introspection, and schema match against the FeatureView.

## Step 4: Retrieve historical features and save datasets

```python
from feast import FeatureStore

store = FeatureStore(".")

job = store.get_historical_features(
    entity_df=entity_df,
    features=["eval_records:expected_response"],
)
training_df = job.to_df()

from feast.data_format import ParquetFormat
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage

storage = SavedDatasetFileStorage(
    path="data/saved_eval.parquet",
    file_format=ParquetFormat(),
)

saved = store.create_saved_dataset(
    from_=job,
    name="eval_training_snapshot",
    storage=storage,
)

loaded = store.get_saved_dataset("eval_training_snapshot")
assert loaded.to_df().shape == training_df.shape
```

Feast primitives in scope: **`get_historical_features()`** and **`SavedDataset`** (via `create_saved_dataset()` / `get_saved_dataset()`).

## Step 5: Dataset sync (Tier 3 offline stores)

For BigQuery, Snowflake, or Redshift, sync MLflow GenAI data into the offline store first:

```bash
feast mlflow sync-dataset --feature-view eval_records
feast mlflow sync-dataset --feature-view eval_records --dry-run
feast mlflow preview-dataset --source production_validation_set --limit 10
```

## Authentication

Feast resolves tokens in this order for DataSource reads:

1. User Bearer token from the current Feast API request (REST / Arrow Flight).
2. `MLFLOW_TRACKING_TOKEN` environment variable.
3. Kubernetes ServiceAccount token file (when present).

Feast registers `FeastMLflowHeaderProvider` as an MLflow `request_header_provider` plugin. When a token is active in context, it sets `Authorization: Bearer <token>` on MLflow REST calls.

### Coexistence with `MLFLOW_TRACKING_AUTH`

The Feast operator may inject `MLFLOW_TRACKING_AUTH=kubernetes-namespaced` for pod-level MLflow access. That uses MLflow’s **auth provider** plugin (ServiceAccount token). Feast’s **header provider** runs in parallel; when a ContextVar token is set (user request or `MLFLOW_TRACKING_TOKEN`), the header provider’s `Authorization` value is used for DataSource reads. Automated jobs without a user token continue to rely on kubernetes-namespaced auth.

## Troubleshooting

| Symptom | What to check |
|---------|----------------|
| `ImportError` for `mlflow` | `pip install 'feast[mlflow]'` |
| Connection / timeout errors | `tracking_uri`, network, `ca_bundle`, MLflow pod health |
| HTTP 401 / 403 | Token validity and MLflow RBAC on the target experiment or artifact |
| Unsupported format | Only `parquet` and `csv` are supported in TP |
| Missing artifact / dataset | Run ID, artifact path, or dataset name; 404 errors include context in the message |

## Limitations (Tech Preview)

- No online store integration with MLflow.
- No pickle or other non-tabular artifact formats.
- No bypass of MLflow’s auth layer via direct object storage.
- Operator FeatureStore CR fields for DataSource-specific MLflow config may evolve in follow-up releases.
