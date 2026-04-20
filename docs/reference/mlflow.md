# MLflow Integration

Feast provides **native integration** with [MLflow](https://mlflow.org/) for automatic feature lineage tracking alongside ML experiments. When enabled, every feature retrieval is logged to the active MLflow run.

## Overview

- **Which features did this model use?** -- auto-logged on every `get_historical_features()` / `get_online_features()` call
- **Which feature service should I use to serve this model?** -- resolved from model URI via `resolve_features()`
- **Can I reproduce the exact training data?** -- entity DataFrame saved as an MLflow artifact
- **Which models break if I change a feature view?** -- reverse index via the Feast UI `/api/mlflow-feature-usage` endpoint
- **When was the feature store last updated?** -- `feast apply` and `feast materialize` logged to a separate ops experiment

### Capabilities

| Capability | How |
|---|---|
| Auto-log feature metadata | Tags on every retrieval inside an active MLflow run |
| Entity DataFrame archival | `entity_df.parquet` artifact for full reproducibility |
| Model registration with lineage | `feast.feature_service` tag propagated to model versions |
| Training-to-prediction linkage | `load_model()` links prediction runs back to training runs |
| Model-to-feature resolution | Map any model URI back to its Feast feature service |
| Operation audit trail | `feast apply` / `feast materialize` logged to `{project}-feast-ops` |
| FeastMlflowClient | Thin wrapper that eliminates direct `import mlflow` in user code |
| Feast UI integration | Per-feature-view usage stats and registered model associations |

## Installation

MLflow is an optional dependency:

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
  tracking_uri: http://127.0.0.1:5000   # optional, falls back to MLFLOW_TRACKING_URI env var
  auto_log: true                         # default
  auto_log_entity_df: false              # default
  entity_df_max_rows: 100000             # default
  log_operations: false                  # default
  ops_experiment_suffix: "-feast-ops"    # default
```

### Configuration options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `false` | Master switch for the entire integration |
| `tracking_uri` | string | *(none)* | MLflow tracking server URI. Falls back to `MLFLOW_TRACKING_URI` env var, then MLflow default (`./mlruns`) |
| `auto_log` | bool | `true` | Automatically log feature metadata on every retrieval when an active MLflow run exists |
| `auto_log_entity_df` | bool | `false` | Save the entity DataFrame as `entity_df.parquet` artifact on historical retrieval |
| `entity_df_max_rows` | int | `100000` | Skip entity DataFrame artifact upload for DataFrames exceeding this limit |
| `log_operations` | bool | `false` | Log `feast apply` and `feast materialize` to a separate MLflow experiment |
| `ops_experiment_suffix` | string | `"-feast-ops"` | Suffix appended to project name for the operations experiment |

### Tracking URI resolution

The tracking URI is resolved in this order:

1. `tracking_uri` field in `feature_store.yaml`
2. `MLFLOW_TRACKING_URI` environment variable
3. MLflow's default (`./mlruns` local directory)

This means you can omit `tracking_uri` from the YAML and set `MLFLOW_TRACKING_URI` in your environment instead.

## What gets logged

### Tags on retrieval runs

When `auto_log: true` and an active MLflow run exists, each `get_historical_features()` or `get_online_features()` call records:

| Tag | Example | Description |
|-----|---------|-------------|
| `feast.project` | `my_project` | Feast project name |
| `feast.retrieval_type` | `historical` / `online` | Type of feature retrieval |
| `feast.feature_service` | `driver_activity_v1` | Auto-resolved feature service name (if matched) |
| `feast.feature_views` | `driver_hourly_stats` | Comma-separated feature view names |
| `feast.feature_refs` | `driver_hourly_stats:conv_rate,...` | All feature references |
| `feast.entity_count` | `200` | Number of entities in the request |
| `feast.feature_count` | `5` | Number of features retrieved |

### Metrics

| Metric | Example | Description |
|--------|---------|-------------|
| `feast.job_submission_sec` | `0.4321` | Feature retrieval duration in seconds |

### Artifacts

When `auto_log_entity_df: true` and the entity DataFrame has fewer than `entity_df_max_rows` rows:

| Artifact | Description |
|----------|-------------|
| `entity_df.parquet` | Full entity DataFrame used in the retrieval |

When a model is logged via `client.log_model()`:

| Artifact | Description |
|----------|-------------|
| `required_features.json` | JSON list of feature references the model was trained on |

### Entity DataFrame metadata

Regardless of `auto_log_entity_df`, the following metadata is logged when present:

| Tag / Param | When | Description |
|-------------|------|-------------|
| `feast.entity_df_type` | Always | `dataframe`, `sql`, or `range` |
| `feast.entity_df_rows` | DataFrame input | Row count |
| `feast.entity_df_columns` | DataFrame input | Column names |
| `feast.entity_df_query` | SQL input | The SQL query string |
| `feast.start_date` / `feast.end_date` | Range-based input | Date range |

### Operation logs

When `log_operations: true`, `feast apply` and `feast materialize` create self-contained runs in the `{project}{ops_experiment_suffix}` experiment (default: `my_project-feast-ops`):

**Apply runs:**

| Tag / Metric | Example |
|--------------|---------|
| `feast.operation` | `apply` |
| `feast.project` | `my_project` |
| `feast.feature_views_changed` | `driver_hourly_stats,order_stats` |
| `feast.feature_services_changed` | `driver_activity_v1` |
| `feast.entities_changed` | `driver,restaurant` |
| `feast.apply.feature_views_count` | `2` |
| `feast.apply.feature_services_count` | `1` |
| `feast.apply.entities_count` | `2` |

**Materialize runs:**

| Tag / Metric | Example |
|--------------|---------|
| `feast.operation` | `materialize` / `materialize_incremental` |
| `feast.project` | `my_project` |
| `feast.materialize.feature_views` | `driver_hourly_stats` |
| `feast.materialize.start_date` | `2024-01-01T00:00:00` |
| `feast.materialize.end_date` | `2024-01-02T00:00:00` |
| `feast.materialize.duration_sec` | `12.3456` |

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
    # The run is now tagged with feast.feature_refs, feast.feature_views, etc.

    model = train(training_df)
    mlflow.sklearn.log_model(model, "model")
```

No extra code needed -- the tags are written automatically.

### FeastMlflowClient (zero `import mlflow`)

The `FeastMlflowClient` wraps MLflow so user code never needs `import mlflow`. All configuration is inherited from `feature_store.yaml`:

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
    client.log_model(model, "model")          # also saves required_features.json
    train_run_id = client.active_run_id

# Register model (auto-tags version with feast.feature_service)
client.register_model(f"runs:/{train_run_id}/model", "driver_model")

# Prediction (auto-links to training run)
with client.start_run(run_name="prediction"):
    model = client.load_model("models:/driver_model/1")
    # This run is now tagged with:
    #   feast.training_run_id → points to train_run_id
    #   feast.model_name → "driver_model"
    #   feast.model_version → "1"
    #   feast.feature_service → copied from training run
    online_features = store.get_online_features(...).to_dict()
    predictions = model.predict(...)
```

### FeastMlflowClient API reference

| Method | Description |
|--------|-------------|
| `store.get_mlflow_client()` | Create a client from the FeatureStore |
| `client.start_run(run_name, tags)` | Context manager; auto-tags run with `feast.project` |
| `client.log_params(params)` | Log parameters to the active run |
| `client.log_metrics(metrics, step)` | Log metrics to the active run |
| `client.log_metric(key, value, step)` | Log a single metric |
| `client.log_model(model, path, flavor)` | Log model + auto-attach `required_features.json` |
| `client.load_model(model_uri)` | Load model + auto-tag prediction run with training lineage |
| `client.register_model(model_uri, name)` | Register model + auto-tag version with `feast.feature_service` |
| `client.resolve_features(model_uri)` | Resolve model URI to Feast feature service name |
| `client.get_training_entity_df(run_id, timestamp_column, max_rows)` | Recover entity DataFrame from a past MLflow run |
| `client.mlflow` | Escape hatch: access the raw `mlflow` module |
| `client.active_run_id` | Current active MLflow run ID (or `None`) |

**Supported model flavors for `log_model()`:** `sklearn`, `pytorch`, `xgboost`, `lightgbm`, `tensorflow`, `keras`, `pyfunc`.

### Resolve a model back to its feature service

```python
client = store.get_mlflow_client()
fs_name = client.resolve_features("models:/driver_model/1")
# Returns: "driver_activity_v1"
```

Resolution order:
1. Model version tag `feast.feature_service` (set by `register_model()`)
2. Training run tag `feast.feature_service` (set by auto-logging)

### Reproduce training from a past run

```python
client = store.get_mlflow_client()
entity_df = client.get_training_entity_df(run_id="abc123")

with client.start_run(run_name="retrain_v2"):
    new_df = store.get_historical_features(
        features=store.get_feature_service("driver_activity_v1"),
        entity_df=entity_df,
    ).to_df()
    model = train(new_df)
    client.log_model(model, "model")
```

This requires `auto_log_entity_df: true` to have been enabled when the original run was recorded.

## Feast UI integration

The Feast UI server exposes three API endpoints that aggregate data from MLflow:

| Endpoint | Description |
|----------|-------------|
| `/api/mlflow-runs` | All Feast-tagged MLflow runs with linked registered models |
| `/api/mlflow-feature-usage` | Per-feature-view usage stats (run count, last used, associated models) |
| `/api/mlflow-feature-models` | Reverse index of feature refs to registered models |

The feature view detail page in the Feast UI displays:
- **MLflow Training Runs** count and **Last Used** date in the header stats
- An **MLflow Usage** panel showing training run count, relative last-used time, and a table of registered models that depend on the feature view

Start the Feast UI with:

```bash
feast ui --host 127.0.0.1 --port 8888
```

