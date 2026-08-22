# MLflow Integration (RHOAI / ODH)

## Overview

When both the Feast operator and MLflow operator are enabled (`Managed`) on RHOAI/ODH, the Feast operator automatically detects the cluster MLflow instance and enables MLflow experiment tracking for every FeatureStore deployment. This provides:

- **Zero-config MLflow lineage** for workbench users (no YAML editing)
- **Feast UI lineage panels** (training runs, model associations, registry graph) via the operator-managed UI Route
- **Operations audit trail** for `feast apply` and `feast materialize`

## Auto-discovery

The operator lists all MLflow CRs (`mlflow.opendatahub.io/v1`) in the cluster and uses the first one with an `Available=True` or `Ready=True` condition. The RHOAI MLflow CRD enforces a singleton named `mlflow`, but the operator uses list-based discovery for forward-compatibility.

If the MLflow CR does not report conditions (older operator versions), auto-discovery will not activate. In that case, set `trackingUri` explicitly.

## FeatureStore CR configuration

### Auto-enabled (default when MLflow is present)

No `spec.mlflow` needed. The operator auto-enables when an Available MLflow CR is detected:

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
    uiUrl: "https://dashboard.example.com/mlflow"
    trackingAuth: "kubernetes-namespaced"
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
| `trackingUri` | string | auto-discovered | MLflow tracking server URI (in-cluster, from `status.address.url`) |
| `uiUrl` | string | auto-discovered | Browser-reachable MLflow URL for Feast UI lineage hyperlinks (from `status.url`) |
| `trackingAuth` | *string | `"kubernetes-namespaced"` | Auth method for Feast pods calling MLflow (see [Authentication](#authentication)) |
| `autoLog` | *bool | `true` | Auto-log feature metadata on every retrieval |
| `autoLogEntityDf` | *bool | `false` | Save entity DataFrame as artifact |
| `entityDfMaxRows` | *int32 | `100000` | Skip artifact for large DataFrames |
| `logOperations` | *bool | `false` | Log `feast apply` / `materialize` to ops experiment |
| `opsExperimentSuffix` | *string | `"-feast-ops"` | Ops experiment name suffix |
| `extraConfig` | map[string]string | — | Additional YAML fields (coerced to native types) |

## Authentication

The operator injects `MLFLOW_TRACKING_AUTH` into all Feast pod containers. This env var is consumed by the MLflow Python client's auth plugin system to attach credentials to HTTP requests made to the tracking server.

| `trackingAuth` value | Behavior |
|---------------------|----------|
| `"kubernetes-namespaced"` (default) | Reads the pod's SA token and namespace from `/var/run/secrets/kubernetes.io/serviceaccount/`, sends `Authorization: Bearer <token>` and `X-MLFLOW-WORKSPACE: <namespace>` headers. Multi-tenant isolation on RHOAI. |
| `"kubernetes"` | Same as above but without the workspace header. Single-tenant setups. |
| `"basic"` | HTTP Basic auth using `MLFLOW_TRACKING_USERNAME` / `MLFLOW_TRACKING_PASSWORD` env vars. |
| `"bearer"` | Static bearer token from `MLFLOW_TRACKING_TOKEN` env var. |
| `""` (empty string) | No auth header. For local dev or unprotected MLflow instances. |

No Kubernetes RoleBinding is required for MLflow tracking API access. The MLflow server validates the SA token directly via TokenReview and applies its own access policies.

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
2. Auto-discovered from MLflow CR `status.address.url` (first Available/Ready CR)
3. `MLFLOW_TRACKING_URI` environment variable (on workbench pods, injected by MLflow operator)
4. MLflow default (`./mlruns`)

## UI URL resolution order (for browser hyperlinks in Feast UI lineage)

1. Explicit `uiUrl` in FeatureStore CR
2. `MLFLOW_UI_URL` environment variable
3. Auto-discovered from MLflow CR `status.url` (external gateway route)
4. Falls back to `trackingUri` (works for local dev where tracking URI is browser-reachable)

## Graceful degradation

- If MLflow operator is not installed: no mlflow block in YAML; FeatureStore stays Ready.
- If no MLflow CR has `Available=True` or `Ready=True` condition: discovery returns empty; mlflow stays off.
- If tracking URI becomes unreachable: SDK logs a warning but does not block feature retrieval.
- If UI pod lacks RBAC: `/api/mlflow-*` returns empty responses; lineage panels are hidden.
