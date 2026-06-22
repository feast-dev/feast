# MLflow Integration

Feast provides **native integration** with [MLflow](https://mlflow.org/) for automatic feature lineage tracking alongside ML experiments. When enabled, every feature retrieval is logged to the active MLflow run.

## Overview

- **Which features did this model use?** -- auto-logged on every `get_historical_features()` / `get_online_features()` call
- **Which feature service should I use to serve this model?** -- resolved from model URI via `store.mlflow.resolve_features()`
- **Can I reproduce the exact training data?** -- entity DataFrame saved as an MLflow artifact
- **Which models break if I change a feature view?** -- reverse index via the Feast UI `/api/mlflow-feature-usage` endpoint
- **When was the feature store last updated?** -- `feast apply` and `feast materialize` logged to a separate ops experiment

### Capabilities

| Capability | How |
|---|---|
| Auto-log feature metadata | Tags on every retrieval inside an active MLflow run |
| Entity DataFrame archival | `entity_df.parquet` artifact for full reproducibility |
| Model registration with lineage | `feast.feature_service` tag propagated to model versions |
| Training-to-prediction linkage | `store.mlflow.load_model()` links prediction runs back to training runs |
| Model-to-feature resolution | Map any model URI back to its Feast feature service |
| Operation audit trail | `feast apply` / `feast materialize` logged to `{project}-feast-ops` |
| `store.mlflow` API | Single entry point — zero `import mlflow`, zero client objects |
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
  enable_distributed_tracing: true       # default
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
| `enable_distributed_tracing` | bool | `true` | When enabled, server-side API calls create MLflow trace spans. Supports parent-child linking via `traceparent` headers. See [Distributed tracing](#distributed-tracing) |

### Tracking URI resolution

The tracking URI is resolved in this order:

1. `tracking_uri` field in `feature_store.yaml`
2. `MLFLOW_TRACKING_URI` environment variable
3. MLflow's default (`./mlruns` local directory)

This means you can omit `tracking_uri` from the YAML and set `MLFLOW_TRACKING_URI` in your environment instead, or it would be pulled from `./mlruns` automatically when both are not set.

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

When a model is logged via `store.mlflow.log_model()`:

| Artifact | Description |
|----------|-------------|
| `feast_features.json` | JSON list of feature references the model was trained on |

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

### Automatic logging (zero code)

With the configuration above, feature metadata is logged automatically whenever there is an active MLflow run. No explicit `import mlflow` is needed — just use `store.mlflow`:

```python
from feast import FeatureStore

store = FeatureStore(".")

with store.mlflow.start_run(run_name="my_training"):
    training_df = store.get_historical_features(
        features=store.get_feature_service("driver_activity_v1"),
        entity_df=entity_df,
    ).to_df()
    # The run is now tagged with feast.feature_refs, feast.feature_views, etc.

    model = train(training_df)
    store.mlflow.log_model(model, "model")
```

No extra code needed — the tags are written automatically.

### `store.mlflow` API (recommended)

`store.mlflow` is the primary way to interact with the Feast–MLflow integration. It provides Feast-enhanced versions of common MLflow operations, and delegates everything else to the raw `mlflow` module:

```python
from feast import FeatureStore
from sklearn.linear_model import LogisticRegression

store = FeatureStore(".")

# Training
with store.mlflow.start_run(run_name="v1_training"):
    df = store.get_historical_features(
        features=store.get_feature_service("driver_activity_v1"),
        entity_df=entity_df,
    ).to_df()

    model = LogisticRegression().fit(X, y)
    store.mlflow.log_model(model, "model")     # Feast-enhanced: saves feast_features.json
    train_run_id = store.mlflow.active_run_id

# Register model (auto-tags version with feast.feature_service)
store.mlflow.register_model(f"runs:/{train_run_id}/model", "driver_model")

# Prediction (auto-links to training run)
with store.mlflow.start_run(run_name="prediction"):
    model = store.mlflow.load_model("models:/driver_model/1")
    online_features = store.get_online_features(
        features=store.get_feature_service("driver_activity_v1"),
        entity_rows=[{"driver_id": 1001}],
    )
    predictions = model.predict(...)
```

### `feast.mlflow` module API (alternative)

For users who prefer a module-level import, `feast.mlflow` is a **drop-in replacement for `import mlflow`** that delegates to the same `store.mlflow` client under the hood:

```python
import feast.mlflow
from feast import FeatureStore

store = FeatureStore(".")   # auto-registers with feast.mlflow

with feast.mlflow.start_run(run_name="training"):
    df = store.get_historical_features(...).to_df()
    feast.mlflow.log_params({"lr": "0.01"})     # plain passthrough
    feast.mlflow.log_metrics({"f1": 0.85})       # plain passthrough
    feast.mlflow.log_model(model, "model")       # Feast-enhanced
```

#### Store resolution

`feast.mlflow` resolves its `FeatureStore` in this order:

1. **Explicit `feast.mlflow.init(store)`** — if called, overrides everything
2. **Auto-registered** — the most recently created `FeatureStore` with `mlflow.enabled=true` registers itself automatically
3. **Auto-discovery** — falls back to `FeatureStore(".")` from the current directory

In most cases, simply creating a `FeatureStore(...)` is enough — no `init()` needed.

#### Error handling

`feast.mlflow` raises clear errors on first use if something is misconfigured:

| Condition | Error |
|-----------|-------|
| No `feature_store.yaml` in cwd and no store created | `RuntimeError` with guidance to call `feast.mlflow.init(store)` |
| `mlflow.enabled` is not set to `true` | `RuntimeError` with guidance to set `mlflow.enabled=true` |
| `mlflow` pip package not installed | `ImportError` with guidance to run `pip install feast[mlflow]` |

When `mlflow.enabled` is `false` (or omitted), `store.mlflow` returns `None`, allowing callers to guard with `if store.mlflow:`. The `feast.mlflow` module raises `RuntimeError` only when you attempt to use it without an enabled store.

### Feast-enhanced functions

These functions add automatic Feast tagging and lineage on top of their MLflow counterparts:

| Function | Enhancement |
|----------|-------------|
| `store.mlflow.start_run(run_name, tags)` | Auto-tags run with `feast.project` |
| `store.mlflow.log_model(model, path, flavor)` | Auto-attaches `feast_features.json` artifact |
| `store.mlflow.register_model(model_uri, name)` | Auto-tags model version with `feast.feature_service` |
| `store.mlflow.load_model(model_uri)` | Auto-tags prediction run with training lineage |

**Supported model flavors for `log_model()`:** `sklearn`, `pytorch`, `xgboost`, `lightgbm`, `tensorflow`, `keras`, `pyfunc`.

### Feast-only functions

These are unique to the Feast integration and have no `mlflow` equivalent:

| Function | Description |
|----------|-------------|
| `store.mlflow.resolve_features(model_uri)` | Resolve model URI to Feast feature service name |
| `store.mlflow.get_training_entity_df(run_id, ...)` | Recover entity DataFrame from a past MLflow run |
| `store.mlflow.log_training_dataset(df, dataset_name)` | Log a training DataFrame as an MLflow dataset input |
| `store.mlflow.active_run_id` | Current active MLflow run ID (or `None`) |
| `store.mlflow.client` | The underlying `MlflowClient` instance for advanced queries |
| `feast.mlflow.init(store)` | Explicitly bind `feast.mlflow` module to a `FeatureStore` (optional) |

### Passthrough behavior

The `feast.mlflow` module delegates any attribute not listed above to the raw `mlflow` module. This means you can use `feast.mlflow` as a drop-in replacement for `import mlflow`:

```python
feast.mlflow.log_params(params)             # passes through to mlflow.log_params
feast.mlflow.log_metrics(metrics)
feast.mlflow.set_tag("env", "staging")
feast.mlflow.MlflowClient()
```

`store.mlflow` does **not** have this passthrough — it only exposes the Feast-enhanced and Feast-only methods listed above. To access raw `mlflow` functions from `store.mlflow`, use the escape hatches:

```python
store.mlflow.client.log_param(run_id, "lr", "0.01")  # via MlflowClient instance
store.mlflow.mlflow.log_params(params)                # via raw mlflow module
```

### Resolve a model back to its feature service

```python
from feast import FeatureStore

store = FeatureStore(".")
fs_name = store.mlflow.resolve_features("models:/driver_model/1")
# Returns: "driver_activity_v1"
```

Resolution order:
1. Model version tag `feast.feature_service` (set by `register_model()`)
2. Training run tag `feast.feature_service` (set by auto-logging)

### Reproduce training from a past run

```python
from feast import FeatureStore

store = FeatureStore(".")

entity_df = store.mlflow.get_training_entity_df(run_id="abc123")

with store.mlflow.start_run(run_name="retrain_v2"):
    new_df = store.get_historical_features(
        features=store.get_feature_service("driver_activity_v1"),
        entity_df=entity_df,
    ).to_df()
    model = train(new_df)
    store.mlflow.log_model(model, "model")
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

## Distributed tracing

When `enable_distributed_tracing` is `true` (the default when `mlflow.enabled=true`), server-side API calls create MLflow trace spans via `mlflow.start_span()`. Spans appear in the MLflow UI **Traces** tab and support parent-child linking via W3C `traceparent` headers.

All feature server endpoints will emit MLflow traces automatically.

### Automatic server traces (MCP & HTTP clients)

When any client (Cursor, Claude Desktop, or direct HTTP) calls the Feast feature server, traces are created automatically with no client-side code changes.

Each trace includes:

| Span attribute | Description |
|----------------|-------------|
| `feast.feature_refs` | Which features were served |
| `feast.entity_count` | Number of entities in the request |
| `feast.project` | Project name |
| `feast.retrieval_type` | `online` |
| `feast.mcp_session_id` | MCP session identifier (when called via MCP) |

The `mcp_session_id` groups all tool calls from a single MCP session (e.g., one Cursor chat conversation), allowing you to filter and correlate all feature retrievals made during that session.

**MCP client configuration** (`mcp.json`):

```json
{
  "mcpServers": {
    "feast": {
      "url": "http://127.0.0.1:6567/mcp"
    }
  }
}
```

**Result:** Independent traces per request in MLflow, filterable by `feast.mcp_session_id`.

### Cross-process trace linking (HTTP agents)

When your agent calls the Feast feature server over HTTP, pass the `traceparent` header to produce a unified trace tree in MLflow.

```python
import mlflow
import requests

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.openai.autolog()

@mlflow.trace
def my_agent(entity_id: int):
    headers = mlflow.tracing.get_tracing_context_headers_for_http_request()

    resp = requests.post(
        "http://feast-server:6567/get-online-features",
        json={"features": [...], "entities": {...}},
        headers=headers,
    )
    features = resp.json()

    response = llm.chat.completions.create(...)
    return response.choices[0].message.content
```

**Result:** A single MLflow trace containing both agent spans and Feast server spans.

### In-process feature context tagging (SDK usage)

When using Feast as a Python library, use `feast_trace_scope` to capture which features were retrieved and attach that metadata to the LLM span.

```python
import mlflow
from feast import FeatureStore
from feast.tracing_context import feast_trace_scope

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.openai.autolog()

store = FeatureStore(repo_path=".")

@mlflow.trace
def my_agent(entity_id: int):
    feature_refs = ["my_view:feature_a", "my_view:feature_b"]

    with feast_trace_scope() as ctx:
        features = store.get_online_features(
            features=feature_refs,
            entity_rows=[{"entity_id": entity_id}],
        )
        ctx.push_retrieval(feature_refs)
        context_attrs = ctx.get_context_attributes()

    with mlflow.start_span(name="llm_call") as span:
        span.set_attributes(context_attrs)
        response = llm.chat.completions.create(...)

    return response.choices[0].message.content
```

**Result:** The LLM span in MLflow contains:

- `feast.context_features` — which features were retrieved
- `feast.context_feature_views` — which feature views were queried
- `feast.context_feature_count` — number of features

### Automatic LLM span tagging

Instead of manually calling `ctx.get_context_attributes()` and `span.set_attributes(...)`, you can install the Feast span processor once at agent startup. It automatically tags any `LLM` / `CHAT_MODEL` span with Feast feature context when a `FeastTraceContext` is active:

```python
from feast.tracing_hooks import install_feast_span_processor

install_feast_span_processor()
```

With this installed, the agent code simplifies to:

```python
import mlflow
from feast import FeatureStore
from feast.tracing_context import feast_trace_scope
from feast.tracing_hooks import install_feast_span_processor

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.openai.autolog()
install_feast_span_processor()

store = FeatureStore(repo_path=".")

@mlflow.trace
def my_agent(entity_id: int):
    feature_refs = ["my_view:feature_a", "my_view:feature_b"]

    with feast_trace_scope() as ctx:
        features = store.get_online_features(
            features=feature_refs,
            entity_rows=[{"entity_id": entity_id}],
        )
        ctx.push_retrieval(feature_refs)

    response = llm.chat.completions.create(...)
    return response.choices[0].message.content
```

The LLM span is automatically tagged with `feast.context_*` attributes — no manual `set_attributes` call needed.

### Tracing API reference

| Function | Description |
|----------|-------------|
| `mlflow.tracing.get_tracing_context_headers_for_http_request()` | Returns `dict` with `traceparent` header for cross-process linking |
| `feast_trace_scope()` | Context manager that creates a `FeastTraceContext` (clears on exit) |
| `ctx.push_retrieval(feature_refs)` | Records retrieved feature references |
| `ctx.get_context_attributes()` | Returns span-ready `dict` of accumulated metadata |
| `install_feast_span_processor()` | Registers a span processor that auto-tags LLM spans with Feast context |

**Important:** Call `ctx.get_context_attributes()` inside the `with feast_trace_scope()` block. The context is cleared on scope exit.

## Dataset sync: MLflow GenAI Datasets → Feast

The `feast datasets sync` command pulls records from an [MLflow GenAI EvaluationDataset](https://mlflow.org/docs/latest/llms/genai/index.html), flattens their nested structure, and pushes them into Feast online and offline stores.

### Feature repository definition

```python
# feature_repo/mlflow_labels.py
from datetime import timedelta

from feast import Entity, FeatureView, Field, PushSource
from feast.types import String

dataset_record = Entity(name="dataset_record", join_keys=["dataset_record_id"])

mlflow_dataset_push = PushSource(name="mlflow_dataset_push")

mlflow_labels_view = FeatureView(
    name="mlflow_labels",
    entities=[dataset_record],
    ttl=timedelta(days=0),
    schema=[
        Field(name="trace_id", dtype=String),
        Field(name="input_question", dtype=String),
        Field(name="expected_response", dtype=String),
        Field(name="guidelines", dtype=String),
    ],
    source=mlflow_dataset_push,
    online=True,
)
```

### Configuration

Add the `dataset_sync` section under `mlflow` in `feature_store.yaml`:

```yaml
mlflow:
  enabled: true
  tracking_uri: http://mlflow:5000
  dataset_sync:
    default_field_mapping:
      "expectations.expected_response": "corrected_response"
      "source.trace.trace_id": "trace_id"
    watermark_key: "feast_last_sync_time"
```

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `dataset_sync.default_field_mapping` | dict | `{}` | Default field mapping overrides (dot-delimited MLflow paths → Feast column names) |
| `dataset_sync.watermark_key` | string | `"feast_last_sync_time"` | MLflow dataset tag key for incremental sync tracking |
| `dataset_sync.default_batch_size` | int | `10000` | Batch size for `write_to_online_store` during sync |

### CLI commands

```bash
# Full sync (all records)
feast datasets sync --source agent-feedback-v3 --feature-view mlflow_labels --full-refresh

# Incremental sync (only new/updated since last sync)
feast datasets sync --source agent-feedback-v3 --feature-view mlflow_labels

# Preview flattened records without writing
feast datasets preview --source agent-feedback-v3 --limit 10

# Dry run (fetch & flatten, no writes)
feast datasets sync --source agent-feedback-v3 --feature-view mlflow_labels --dry-run

# Custom field mapping from a JSON file
feast datasets sync --source agent-feedback-v3 --feature-view mlflow_labels --field-mapping mapping.json
```

### Default flattening rules

| MLflow path | Feast column |
|---|---|
| `inputs.X` | `input_X` |
| `expectations.X` | `X` |
| `source.trace.trace_id` | `trace_id` |
| `tags.X` | `tag_X` |
| `last_update_time` | `event_timestamp` |

Override with a `--field-mapping` JSON file or `dataset_sync.default_field_mapping` in config.

### Incremental sync strategy

After each sync, the command tags the MLflow dataset with the configured watermark key. On the next incremental run, only records with `last_update_time` newer than the watermark are ingested. Use `--full-refresh` to ignore the watermark.

### Retrieving synced features

**Online:**

```python
store = FeatureStore(repo_path="feature_repo/")
features = store.get_online_features(
    features=["mlflow_labels:expected_response", "mlflow_labels:guidelines"],
    entity_rows=[{"dataset_record_id": "rec-abc123"}],
).to_dict()
```

**Offline (batch):**

```python
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=["mlflow_labels:expected_response", "mlflow_labels:guidelines"],
).to_df()
```
