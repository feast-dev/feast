# Feast x MLflow Integration -- Design Document

## Overview

This document describes the phased design for integrating MLflow experiment
tracking into Feast. The goal is to bridge the gap between feature engineering
(Feast) and model training/serving (MLflow), enabling automatic lineage tracking,
training reproducibility, model-to-feature traceability, and full operation
visibility -- all with minimal user configuration.

### User Configuration

A single `mlflow:` block in `feature_store.yaml`:

```yaml
mlflow:
  enabled: true
  tracking_uri: http://127.0.0.1:5000   # optional, falls back to MLFLOW_TRACKING_URI env
  auto_log: true                         # default
  auto_log_entity_df: false              # default
  entity_df_max_rows: 100000             # default
  log_execution_context: true            # default (Phase 6)
  log_operations: false                  # default (Phase 7)
  ops_experiment_suffix: "-feast-ops"    # default (Phase 7)
```

**Installation:** `pip install feast[mlflow]` (requires `mlflow>=2.10.0`)

---

## Phase 1: Configuration (DONE)

**Files:** `mlflow_integration/config.py`, `repo_config.py`, `feature_store.py`

`MlflowConfig` is a Pydantic model (`FeastBaseModel`) with the following fields:

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `enabled` | bool | `False` | Master switch for the entire integration |
| `tracking_uri` | Optional[str] | `None` | MLflow tracking server URI. Falls back to `MLFLOW_TRACKING_URI` env var, then MLflow default |
| `auto_log` | bool | `True` | Automatically log feature retrieval metadata when an active MLflow run exists |
| `auto_log_entity_df` | bool | `False` | Persist `entity_df.parquet` as an artifact on historical retrieval |
| `entity_df_max_rows` | int | `100000` | Skip entity DF artifact upload for DataFrames larger than this |

**RepoConfig integration:** A new field `mlflow_config: Optional[Any] = Field(None, alias="mlflow")` is added to `RepoConfig`. A `@property mlflow` lazily constructs `MlflowConfig(**dict)` from the raw YAML data, avoiding importing mlflow at config parse time.

**FeatureStore.__init__:** Calls `_init_mlflow_tracking()`. When `mlflow.enabled` is `True`:
1. Calls `mlflow.set_tracking_uri(tracking_uri)` to configure the global MLflow client
2. Calls `mlflow.set_experiment(self.config.project)` -- the Feast project name becomes the MLflow experiment name

This is a no-op if mlflow is not installed (lazy import, graceful fallback).

---

## Phase 2: Auto-Logging (DONE)

**Files:** `mlflow_integration/logger.py`, `feature_store.py`

### `log_feature_retrieval_to_mlflow()`

Writes the following to the **active** MLflow run (no-op if no run is active):

**Tags:**
| Tag | Example |
|-----|---------|
| `feast.project` | `my_repo` |
| `feast.retrieval_type` | `historical` / `online` |
| `feast.feature_service` | `driver_activity_v1` |
| `feast.feature_views` | `driver_hourly_stats,transformed_conv_rate` |
| `feast.feature_refs` | `driver_hourly_stats:conv_rate,...` |
| `feast.entity_count` | `200` |
| `feast.feature_count` | `3` |

**Metrics:**
| Metric | Example |
|--------|---------|
| `feast.job_submission_sec` | `0.4321` |

**Artifacts (when `auto_log_entity_df: true`):**
| Artifact | Description |
|----------|-------------|
| `entity_df.parquet` | Full entity DataFrame used in the retrieval |

### Integration points in `feature_store.py`

Hooked into `get_historical_features()` and `get_online_features()` after the provider call returns. Both follow the same pattern: check `config.mlflow.enabled and config.mlflow.auto_log`, then call `log_feature_retrieval_to_mlflow()` with graceful fallback.

### Design principles

- **Lazy import:** `mlflow` is never imported until the first logging call via `_get_mlflow()`.
- **Graceful degradation:** A failing MLflow server never blocks feature retrieval. Failures are rate-limited to one warning per 5 minutes.
- **Zero overhead when disabled:** No mlflow import, no network calls, no tags.

---

## Phase 3: Entity DataFrame Recovery (DONE)

**Files:** `mlflow_integration/entity_df_builder.py`

### `get_entity_df_from_mlflow_run(run_id, tracking_uri, timestamp_column, max_rows)`

Loads `entity_df.parquet` (or `.csv`) from a run's artifacts. Validates that the expected timestamp column exists. Supports row limiting via `max_rows`.

Raises `FeastMlflowEntityDfError` if mlflow is not installed, the run doesn't exist, or no entity artifact is found.

---

## Phase 4: Model-to-Feature Resolution (DONE)

**Files:** `mlflow_integration/model_resolver.py`

### `resolve_feature_service_from_model_uri(model_uri, store, tracking_uri)`

Resolves `models:/<name>/<version_or_alias>` to a Feast feature service name.

**Resolution order:**
1. Model version tag `feast.feature_service` (explicit override)
2. Training run tag `feast.feature_service` (set by auto-log)

When `store` is provided, validates that the feature service exists in the registry and optionally checks `feast_features.json` artifact for feature mismatch.

---

## Phase 5: Feast UI Integration (DONE)

**Files:** `ui_server.py`, `ui/src/components/RegistryVisualization.tsx`, `ui/src/queries/useLoadMlflowRuns.ts`

**Backend endpoints:**
- `GET /api/mlflow-runs` -- Lists MLflow runs linked to this Feast project, enriched with registered model info
- `GET /api/mlflow-feature-models` -- Reverse index: feature_ref -> registered models that trained on it

**Frontend:** MLflow run/model nodes in the registry graph visualization with direct links to the MLflow UI.

---

## Phase 6: Execution Context Backtracking (PROPOSED -- not yet implemented)

**Full proposal:** `proposals/phase6_execution_context.md`

### Goal

Tag every auto-logged MLflow run with WHERE the Feast retrieval happened (RHOAI workbench, KFP pipeline step, feature server, or standalone) by reading environment variables that the RHOAI platform already sets on pods.

### Why it's not implemented yet

The exact environment variable names set by KFP and ODH need to be validated on a real RHOAI cluster before writing production code. The env var names vary between KFP v1 and v2, and between ODH versions. Implementation is estimated at 1-2 days once the env var names are confirmed.

### What it will do (when implemented)

- Add `_detect_execution_context()` to `logger.py` that reads env vars like `KFP_RUN_ID`, `NOTEBOOK_NAME`
- Tag MLflow runs with `feast.execution_context`, `feast.kfp_run_id`, `feast.workbench`, `feast.namespace`
- Enable the RHOAI dashboard to deep-link from MLflow runs back to KFP pipeline runs or workbenches

---

## Phase 7: Expanded Operation Visibility (NEW)

**Files:** `mlflow_integration/logger.py`, `mlflow_integration/config.py`, `feature_store.py`

### Goal

Log `feast apply` and `feast materialize` / `feast materialize-incremental` to MLflow, giving visibility into schema changes and data freshness alongside training runs.

### New config fields

- `log_operations: bool = False` (opt-in to avoid noise)
- `ops_experiment_suffix: str = "-feast-ops"` (operations go to a separate experiment)

### New functions in `logger.py`

**`log_apply_to_mlflow(changed_objects, project, tracking_uri)`:**
Creates an auto-closed run in `{project}-feast-ops` with tags for changed feature views, feature services, and entities.

**`log_materialize_to_mlflow(feature_views, start_date, end_date, duration, project, tracking_uri, incremental)`:**
Creates an auto-closed run in `{project}-feast-ops` with tags for the materialization window, feature views materialized, and duration metric.

### Integration in `feature_store.py`

Hooks added parallel to existing OpenLineage hooks after `_apply_diffs()`, `apply()`, `materialize()`, and `materialize_incremental()`.

---

## Phase 8: Registry Enrichment with MLflow Metadata (DONE + PROPOSED extension)

### What's implemented: Query-time enrichment

**Files:** `ui_server.py`, `ui/src/queries/useLoadFeatureUsage.ts`

`GET /api/mlflow-feature-usage` endpoint that queries MLflow for all runs with `feast.feature_refs` tags, aggregates per-feature-view usage stats (run count, last used, linked models), and caches for 5 minutes. The React hook `useLoadFeatureUsage` fetches this endpoint.

### Proposed extension: Write-back to registry

**Full proposal:** `proposals/phase8_registry_writeback.md`

A future extension would write usage stats into the Feast registry itself (new `feature_usage_stats` table or proto field) so data is available offline without requiring a live MLflow connection. This requires a registry schema change and is documented as a proposal pending operator team review.

---

## Phase 9: `feast.mlflow` Module API (NEW)

**Files:** `feast/mlflow.py`, `mlflow_integration/client.py`, `feature_store.py`

### Goal

A module-level API (`feast.mlflow.*`) that eliminates direct `import mlflow` and requires zero client objects. `FeatureStore` auto-registers itself on creation, so `feast.mlflow.start_run()` just works.

### Architecture

- `feast/mlflow.py` — public API. Uses open-delegation `__getattr__`: Feast-enhanced client first, raw `mlflow` module fallback. Drop-in replacement for `import mlflow`.
- `mlflow_integration/client.py` — internal `FeastMlflowClient` class. Contains **only** Feast-enhanced methods. No plain passthrough wrappers.
- `feature_store.py` — `_register_with_mlflow_module()` called at end of `__init__` to auto-register the store.

### Delegation Model

```
feast.mlflow.start_run()       → FeastMlflowClient.start_run()     (Feast-enhanced)
feast.mlflow.log_model()       → FeastMlflowClient.log_model()     (Feast-enhanced)
feast.mlflow.log_params()      → mlflow.log_params()               (raw passthrough)
feast.mlflow.MlflowClient()   → mlflow.MlflowClient()             (raw passthrough)
feast.mlflow.sklearn           → mlflow.sklearn                     (raw passthrough)
```

### Store Resolution

1. Explicit `feast.mlflow.init(store)` (override)
2. Most recently created `FeatureStore` with `mlflow.enabled=true` (auto-registered)
3. `FeatureStore(".")` from current directory (fallback)

### Training-to-Prediction Run Linkage

`load_model()` closes the training→prediction loop by auto-tagging the prediction run with:
- `feast.training_run_id` -- the training run that produced the model
- `feast.model_name` -- registered model name
- `feast.model_version` -- model version
- `feast.feature_service` -- copied from the training run

### Feast-enhanced functions

| Function | Enhancement |
|----------|-------------|
| `feast.mlflow.start_run(run_name, tags)` | Auto-tags `feast.project` |
| `feast.mlflow.log_model(model, path, flavor)` | Auto-attaches `feast_features.json` |
| `feast.mlflow.register_model(model_uri, name)` | Auto-tags version with `feast.feature_service` |
| `feast.mlflow.load_model(model_uri)` | Auto-tags prediction run with training lineage |

### Feast-only functions

| Function | Purpose |
|----------|---------|
| `feast.mlflow.init(store)` | Explicitly bind to a store (optional) |
| `feast.mlflow.resolve_features(model_uri)` | Resolve model URI to feature service name |
| `feast.mlflow.get_training_entity_df(run_id)` | Recover entity DataFrame from a past run |
| `feast.mlflow.get_active_run_id()` | Current active run ID (or `None`) |
| `feast.mlflow.active_run_id` | Property shorthand for above |

### Passthrough

All other `feast.mlflow.*` calls delegate to the raw `mlflow` module unchanged. No escape hatch needed.

---

## Operator Gap (Documented, Out of Scope)

The Feast operator's Go `RepoConfig` struct has no `mlflow` field. For RHOAI:
- Use `spec.services.*.server.env` to set `MLFLOW_TRACKING_URI` as an env var
- Use git-based `feastProjectDir` with a pre-baked `feature_store.yaml`
- A follow-up PR would add an `Mlflow` field to the CRD and wire it through `repo_config.go`
