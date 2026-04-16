# MLflow Integration

This module provides **native integration** between Feast and [MLflow](https://mlflow.org/), enabling automatic feature lineage tracking alongside your ML experiments. When enabled, every feature retrieval is logged to the active MLflow run.

## Overview

When enabled, the integration logs to the active MLflow run during:

- **Historical feature retrieval** — `get_historical_features()` tags the run with feature refs, feature views, entity count, and retrieval duration
- **Online feature retrieval** — `get_online_features()` tags the run with the same metadata
- **Entity DataFrame archival** — optionally saves the training entity DataFrame as an MLflow artifact for full reproducibility

The integration also provides utilities for:

- **Model → Feature Service resolution** — map any MLflow model URI back to its Feast feature service
- **Training reproducibility** — reconstruct the exact entity DataFrame from a past MLflow run

## Installation

MLflow is an optional dependency. Install it with:

```bash
pip install mlflow
```

## Configuration

Add the `mlflow` section to your `feature_store.yaml`:

```yaml
project: my_project
registry: data/registry.db
provider: local
online_store:
  type: sqlite
  path: data/online_store.db

mlflow:
  enabled: true
  tracking_uri: https://mlflow.example.com   # or set MLFLOW_TRACKING_URI env var
  auto_log: true
  auto_log_entity_df: true
  entity_df_max_rows: 100000
```

### Configuration options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `false` | Enable or disable the MLflow integration |
| `tracking_uri` | string | *(none)* | MLflow tracking server URI. When not set, the `MLFLOW_TRACKING_URI` environment variable is used. If neither is set, MLflow falls back to its own default (`./mlruns`). |
| `auto_log` | bool | `true` | Automatically log feature metadata on every retrieval |
| `auto_log_entity_df` | bool | `false` | Save the entity DataFrame as an MLflow artifact (`entity_df.parquet`) |
| `entity_df_max_rows` | int | `100000` | Maximum entity DataFrame rows to save as an artifact. DataFrames exceeding this limit are skipped to avoid OOM and slow uploads. |

## What gets logged

When `auto_log: true`, each `get_historical_features` or `get_online_features` call records the following on the active MLflow run:

### Tags

| Tag | Example | Description |
|-----|---------|-------------|
| `feast.project` | `my_project` | Feast project name |
| `feast.retrieval_type` | `historical` / `online` | Type of feature retrieval |
| `feast.feature_service` | `driver_activity_v1` | Feature service used (if any) |
| `feast.feature_views` | `driver_hourly_stats` | Comma-separated list of feature views |
| `feast.feature_refs` | `driver_hourly_stats:conv_rate,...` | All feature references |
| `feast.entity_count` | `200` | Number of entities in the request |
| `feast.feature_count` | `5` | Number of features retrieved |

### Metrics

| Metric | Example | Description |
|--------|---------|-------------|
| `feast.job_submission_sec` | `0.4321` | Feature retrieval duration in seconds |

### Artifacts

When `auto_log_entity_df: true`, the entity DataFrame is saved as `entity_df.parquet` in the run's artifacts (if the row count is within `entity_df_max_rows`), enabling exact reproduction of training data.

## Usage

### Automatic logging

With the configuration above, feature metadata is logged whenever there is an active MLflow run:

```python
import mlflow
from feast import FeatureStore

store = FeatureStore(".")

with mlflow.start_run(run_name="my_training"):
    training_df = store.get_historical_features(
        features=store.get_feature_service("driver_activity_v1"),
        entity_df=entity_df,
    ).to_df()

    # Feature metadata is already logged to this run — no extra code needed
    model = train(training_df)
    mlflow.sklearn.log_model(model, "model")
```

### Resolve a model back to its feature service

Given an MLflow model URI, determine which Feast feature service was used during training:

```python
from feast.mlflow_integration import resolve_feature_service_from_model_uri

fs_name = resolve_feature_service_from_model_uri("models:/my_model/1")
# Returns "driver_activity_v1" — resolved from the training run's tags
```

Resolution order:
1. Model version tag `feast.feature_service` (explicit override)
2. Training run tag `feast.feature_service` (set by auto-log)

If neither tag is found, a `FeastMlflowModelResolutionError` is raised with guidance on how to set the tag.

### Reproduce training from a past run

Retrieve the exact entity DataFrame that was used in a previous training run:

```python
from feast.mlflow_integration import get_entity_df_from_mlflow_run

entity_df = get_entity_df_from_mlflow_run(run_id="abc123")
# Returns the entity DataFrame saved during the original run

training_df = store.get_historical_features(
    features=store.get_feature_service("driver_activity_v1"),
    entity_df=entity_df,
).to_df()
```

This requires `auto_log_entity_df: true` to have been enabled when the original run was recorded.
