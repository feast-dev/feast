# MLflow Integration

This module provides **native integration** between Feast and [MLflow](https://mlflow.org/), enabling automatic feature lineage tracking alongside your ML experiments. When enabled, every feature retrieval is logged to the active MLflow run.

## Overview

When enabled, the integration provides:

- **Historical feature retrieval** -- `get_historical_features()` tags the run with feature refs, feature views, entity count, and retrieval duration
- **Online feature retrieval** -- `get_online_features()` tags the run with the same metadata
- **Entity DataFrame archival** -- optionally saves the training entity DataFrame as an MLflow artifact for full reproducibility
- **Execution context tagging** -- tags runs with where they ran (workbench, KFP pipeline, feature server, or standalone)
- **Operation logging** -- optionally logs `feast apply` and `feast materialize` to a separate MLflow experiment
- **Model-to-Feature resolution** -- map any MLflow model URI back to its Feast feature service
- **Training reproducibility** -- reconstruct the exact entity DataFrame from a past MLflow run
- **Training-to-prediction linkage** -- `FeastMlflowClient.load_model()` links prediction runs back to their training runs
- **Feast MLflow Client** -- a thin wrapper that eliminates direct `import mlflow` in user code

## Installation

MLflow is an optional dependency. Install it with:

```bash
pip install feast[mlflow]
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
  log_execution_context: true
  log_operations: false
  ops_experiment_suffix: "-feast-ops"
```

### Configuration options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `false` | Enable or disable the MLflow integration |
| `tracking_uri` | string | *(none)* | MLflow tracking server URI. Falls back to `MLFLOW_TRACKING_URI` env var, then MLflow default (`./mlruns`). |
| `auto_log` | bool | `true` | Automatically log feature metadata on every retrieval |
| `auto_log_entity_df` | bool | `false` | Save the entity DataFrame as an MLflow artifact (`entity_df.parquet`) |
| `entity_df_max_rows` | int | `100000` | Maximum entity DataFrame rows to save as an artifact |
| `log_execution_context` | bool | `true` | Tag runs with execution context (pipeline, workbench, feature_server, standalone) |
| `log_operations` | bool | `false` | Log `feast apply` and `feast materialize` to a separate MLflow experiment |
| `ops_experiment_suffix` | string | `"-feast-ops"` | Suffix for the operations experiment name |

## What gets logged

### Tags on retrieval runs

When `auto_log: true`, each `get_historical_features` or `get_online_features` call records:

| Tag | Example | Description |
|-----|---------|-------------|
| `feast.project` | `my_project` | Feast project name |
| `feast.retrieval_type` | `historical` / `online` | Type of feature retrieval |
| `feast.feature_service` | `driver_activity_v1` | Feature service used (if any) |
| `feast.feature_views` | `driver_hourly_stats` | Comma-separated list of feature views |
| `feast.feature_refs` | `driver_hourly_stats:conv_rate,...` | All feature references |
| `feast.entity_count` | `200` | Number of entities in the request |
| `feast.feature_count` | `5` | Number of features retrieved |

### Execution context tags

When `log_execution_context: true`:

| Tag | When set | Example |
|-----|----------|---------|
| `feast.execution_context` | Always | `pipeline` / `workbench` / `feature_server` / `standalone` |
| `feast.kfp_run_id` | Pipeline (KFP) | `abc-123-def` |
| `feast.kfp_pipeline` | Pipeline (KFP) | `fraud-training-pipeline` |
| `feast.workbench` | RHOAI workbench | `my-jupyter-notebook` |
| `feast.namespace` | Pipeline or workbench | `user-project` |

### Metrics

| Metric | Example | Description |
|--------|---------|-------------|
| `feast.job_submission_sec` | `0.4321` | Feature retrieval duration in seconds |

### Artifacts

When `auto_log_entity_df: true`, the entity DataFrame is saved as `entity_df.parquet`.

### Operation logs (when `log_operations: true`)

`feast apply` and `feast materialize` create runs in the `{project}-feast-ops` experiment:

| Tag | Example |
|-----|---------|
| `feast.operation` | `apply` / `materialize` / `materialize_incremental` |
| `feast.feature_views_changed` | `driver_hourly_stats` (apply only) |
| `feast.materialize.feature_views` | `driver_hourly_stats` (materialize only) |

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

    model = train(training_df)
    mlflow.sklearn.log_model(model, "model")
```

### FeastMlflowClient (zero mlflow imports)

The `FeastMlflowClient` wraps MLflow so user code never needs `import mlflow`:

```python
from feast import FeatureStore

store = FeatureStore(".")
client = store.get_mlflow_client()

# Training
with client.start_run(run_name="v1_training"):
    df = store.get_historical_features(
        features=store.get_feature_service("driver_activity_v1"),
        entity_df=entity_df,
    ).to_df()

    model = LogisticRegression().fit(X, y)
    client.log_params({"model_type": "logistic_regression"})
    client.log_metrics({"f1": 0.85})
    client.log_model(model, "model")
    train_run_id = client.active_run_id

client.register_model(f"runs:/{train_run_id}/model", "driver_model")

# Prediction (auto-links to training run)
with client.start_run(run_name="prediction"):
    model = client.load_model("models:/driver_model/1")
    # This run is now tagged with feast.training_run_id pointing to train_run_id
    online_features = store.get_online_features(...).to_dict()
    predictions = model.predict(...)
```

### FeastMlflowClient API

| Method | Description |
|--------|-------------|
| `store.get_mlflow_client()` | Create a client from the FeatureStore |
| `client.start_run(run_name, tags)` | Context manager, auto-tags `feast.project` |
| `client.log_params(params)` | Log parameters |
| `client.log_metrics(metrics, step)` | Log metrics |
| `client.log_metric(key, value, step)` | Log a single metric |
| `client.log_model(model, path, flavor)` | Log model + auto-attach `required_features.json` |
| `client.load_model(model_uri)` | Load model + auto-tag prediction run with training lineage |
| `client.register_model(model_uri, name)` | Register + auto-tag version with `feast.feature_service` |
| `client.resolve_features(model_uri)` | Resolve model URI to Feast feature service name |
| `client.get_training_entity_df(run_id)` | Recover entity DataFrame from a past run |
| `client.mlflow` | Escape hatch: raw mlflow module |
| `client.active_run_id` | Current active run ID |

### Resolve a model back to its feature service

```python
from feast.mlflow_integration import resolve_feature_service_from_model_uri

fs_name = resolve_feature_service_from_model_uri("models:/my_model/1")
```

Resolution order:
1. Model version tag `feast.feature_service` (explicit override)
2. Training run tag `feast.feature_service` (set by auto-log)

### Reproduce training from a past run

```python
from feast.mlflow_integration import get_entity_df_from_mlflow_run

entity_df = get_entity_df_from_mlflow_run(run_id="abc123")

training_df = store.get_historical_features(
    features=store.get_feature_service("driver_activity_v1"),
    entity_df=entity_df,
).to_df()
```

This requires `auto_log_entity_df: true` to have been enabled when the original run was recorded.
