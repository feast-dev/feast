# Guide 8 — MLflow Integration

The operator auto-discovers MLflow on RHOAI/ODH clusters and enables experiment tracking
for every FeatureStore deployment. When the MLflow operator is present and healthy, Feast
pods receive MLflow configuration automatically — no manual YAML editing required.

---

## Auto-discovery

The operator lists all `MLflow` CRs (`mlflow.opendatahub.io/v1`) in the cluster and uses
the first one with an `Available=True` or `Ready=True` condition. When found, it populates
`tracking_uri` from `status.address.url` and `ui_url` from `status.url`.

If the MLflow CR does not report conditions (older operator versions), auto-discovery will
not activate. Set `trackingUri` explicitly in that case.

> **No MLflow?** The FeatureStore stays Ready. Non-MLflow FeatureViews and all other Feast
> services are completely unaffected.

---

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

---

## Field reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | auto-detected | Master switch for MLflow integration |
| `trackingUri` | string | auto-discovered | MLflow tracking server URI (in-cluster, from `status.address.url`) |
| `uiUrl` | string | auto-discovered | Browser-reachable MLflow URL for Feast UI lineage links (from `status.url`) |
| `trackingAuth` | *string | `"kubernetes-namespaced"` | Auth method for Feast pods calling MLflow |
| `autoLog` | *bool | `true` | Auto-log feature metadata on every retrieval |
| `autoLogEntityDf` | *bool | `false` | Save entity DataFrame as artifact |
| `entityDfMaxRows` | *int32 | `100000` | Skip artifact for large DataFrames |
| `logOperations` | *bool | `false` | Log `feast apply` / `materialize` to ops experiment |
| `opsExperimentSuffix` | *string | `"-feast-ops"` | Ops experiment name suffix |
| `extraConfig` | map[string]string | — | Additional YAML fields (coerced to native types) |

---

## Authentication

The operator injects `MLFLOW_TRACKING_AUTH` into all Feast pod containers. The MLflow
Python client's auth plugin system uses this env var to attach credentials to tracking
server requests.

| `trackingAuth` value | Behavior |
|---------------------|----------|
| `"kubernetes-namespaced"` (default) | SA token + `X-MLFLOW-WORKSPACE: <namespace>` header. Multi-tenant on RHOAI. |
| `"kubernetes"` | SA token only. Single-tenant setups. |
| `"basic"` | HTTP Basic auth via `MLFLOW_TRACKING_USERNAME` / `MLFLOW_TRACKING_PASSWORD` env vars. |
| `"bearer"` | Static bearer token from `MLFLOW_TRACKING_TOKEN` env var. |
| `""` (empty string) | No auth header. Local dev or unprotected MLflow. |

No Kubernetes RoleBinding is needed for MLflow tracking API access. The MLflow server
validates the SA token directly via TokenReview.

---

## Tracking URI resolution order

1. Explicit `trackingUri` in the FeatureStore CR
2. Auto-discovered from MLflow CR `status.address.url` (first Available/Ready CR)
3. `MLFLOW_TRACKING_URI` environment variable (on workbench pods, injected by the MLflow operator)
4. MLflow default (`./mlruns`)

---

## UI URL resolution order

Used for browser hyperlinks in Feast UI lineage panels:

1. Explicit `uiUrl` in the FeatureStore CR
2. `MLFLOW_UI_URL` environment variable
3. Auto-discovered from MLflow CR `status.url` (external gateway route)
4. Falls back to `trackingUri` (works for local dev)

---

## Graceful degradation

| Scenario | Behavior |
|----------|----------|
| MLflow operator not installed | No `mlflow` block in YAML; FeatureStore stays Ready |
| MLflow CR exists but not Ready | Discovery returns empty; MLflow stays off |
| Tracking URI becomes unreachable | SDK logs a warning; feature retrieval is not blocked |
| `spec.mlflow.enabled: false` | MLflow integration explicitly disabled |

---

## Workbench usage

In a RHOAI workbench notebook connected to the FeatureStore:

```python
from feast import FeatureStore

store = FeatureStore(...)  # from mounted client config

with store.mlflow.start_run(run_name="training"):
    df = store.get_historical_features(
        entity_df=entity_df,
        features=["driver_stats:conv_rate", "driver_stats:acc_rate"],
    ).to_df()
    model = train(df)
    store.mlflow.log_model(model, "model")
```

> **Dependency**: the Feast image must include `feast[mlflow]` (`mlflow` or `mlflow-skinny`).

---

## RBAC permissions

The operator needs `get`, `list`, `watch` on `mlflows` in the `mlflow.opendatahub.io` API
group. This is included in the default operator ClusterRole.

```yaml
- apiGroups:
    - mlflow.opendatahub.io
  resources:
    - mlflows
  verbs:
    - get
    - list
    - watch
```

---

## See also

- [API field reference — `MlflowConfig`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md)
- [MLflow DataSource reference](../../reference/mlflow.md)
- [Guide 5 — Security](05-security.md) (RBAC / OIDC auth)
- [Guide 7 — OpenLineage & Materialization](07-openlineage-and-materialization.md)
