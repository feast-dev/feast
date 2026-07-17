# MLflow Integration (RHOAI / ODH)

## Overview

When both the Feast operator and MLflow operator are enabled (`Managed`) on RHOAI/ODH, the Feast operator automatically detects the cluster MLflow instance and enables MLflow experiment tracking for every FeatureStore deployment. This provides:

- **Zero-config MLflow lineage** for workbench users (no YAML editing)
- **Feast UI lineage panels** (training runs, model associations, registry graph) via the operator-managed UI Route
- **Operations audit trail** for `feast apply` and `feast materialize`

## How it works

1. The Feast operator looks for the singleton cluster-scoped `MLflow` CR (`mlflow.opendatahub.io/v1`, name `mlflow`).
2. If found and Ready, the operator reads `status.address.url` (in-cluster HTTPS) as the tracking URI.
3. The `mlflow:` block is written into:
   - **Server `feature_store.yaml`** (all Feast pods: online, offline, registry, UI, cron)
   - **Client ConfigMap** (workbench notebooks)
4. `MLFLOW_TRACKING_AUTH=kubernetes-namespaced` and `MLFLOW_TRACKING_URI` env vars are injected into all Feast service pods.
5. An `mlflow-integration` RoleBinding is created for the FeatureStore ServiceAccount.

## FeatureStore CR configuration

### Auto-enabled (default when MLflow is present)

No `spec.mlflow` needed. The operator auto-enables when MLflow is detected:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: my-store
spec:
  feastProject: my_project
  services:
    onlineStore: {}
    registry: {}
    ui: {}
  # mlflow is auto-enabled — no config required
```

### Explicit configuration

Override defaults or enable additional features:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: my-store
spec:
  feastProject: my_project
  services:
    onlineStore: {}
    registry: {}
    ui: {}
  mlflow:
    enabled: true
    trackingUri: "https://custom-mlflow.example.com:8443"
    autoLog: true
    autoLogEntityDf: true
    entityDfMaxRows: 50000
    logOperations: true
    opsExperimentSuffix: "-feast-ops"
```

### Opt-out

Disable MLflow even when the MLflow operator is present:

```yaml
spec:
  mlflow:
    enabled: false
```

## Configuration options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | auto-detected | Master switch for MLflow integration |
| `trackingUri` | string | auto-discovered | MLflow tracking server URI |
| `autoLog` | *bool | `true` | Auto-log feature metadata on every retrieval |
| `autoLogEntityDf` | *bool | `false` | Save entity DataFrame as artifact |
| `entityDfMaxRows` | *int32 | `100000` | Skip artifact for large DataFrames |
| `logOperations` | *bool | `false` | Log `feast apply` / `materialize` to ops experiment |
| `opsExperimentSuffix` | *string | `"-feast-ops"` | Ops experiment name suffix |
| `extraConfig` | map[string]string | — | Additional YAML fields (coerced to native types) |

## Where to see lineage

Lineage appears in the **Feast UI** (not the RHOAI Dashboard):

1. Open the Feast UI Route: `oc get route -l app.kubernetes.io/name=<featurestore-name>`
2. Feature View page shows MLflow training run count, last used, and model associations.
3. Registry visualization draws edges from FeatureService through MLflow runs to registered models.
4. Click model links to open the RHOAI MLflow UI for that run/model.

The RHOAI Dashboard provides navigation to both the Feast UI and the MLflow application tile.

## Workbench setup

For workbench notebooks to use `store.mlflow`:

1. Select the Feast project in the RHOAI Dashboard (mounts client ConfigMap).
2. Ensure the workbench has the `opendatahub.io/mlflow-instance` annotation (enables MLflow SDK env injection by the MLflow operator).

Then in the notebook:

```python
from feast import FeatureStore

store = FeatureStore(...)  # from mounted client config

with store.mlflow.start_run(run_name="training"):
    df = store.get_historical_features(...).to_df()
    model = train(df)
    store.mlflow.log_model(model, "model")
```

## Tracking URI resolution order

1. Explicit `trackingUri` in FeatureStore CR
2. Auto-discovered from MLflow CR `status.address.url`
3. `MLFLOW_TRACKING_URI` environment variable (on workbench pods, injected by MLflow operator)
4. MLflow default (`./mlruns`)

## Graceful degradation

- If MLflow operator is not installed: no mlflow block in YAML; FeatureStore stays Ready.
- If MLflow CR is not Ready: discovery returns empty; mlflow stays off.
- If tracking URI becomes unreachable: SDK logs a warning but does not block feature retrieval.
- If UI pod lacks RBAC: `/api/mlflow-*` returns empty responses; lineage panels are hidden.
