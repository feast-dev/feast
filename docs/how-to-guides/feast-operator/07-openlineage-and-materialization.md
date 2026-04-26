# Guide 7 — OpenLineage & Materialization

Both `spec.openlineage` and `spec.materialization` are written into `feature_store.yaml`
for **all** service pods — they apply to the online server, offline server, registry, and
materialization jobs alike.

---

## OpenLineage Data Lineage (`spec.openlineage`)

OpenLineage emits data lineage events during `feast apply` (registry changes) and
materialization. Events go outbound from Feast pods to your OpenLineage-compatible backend
([Marquez](https://marquezproject.ai), any OpenLineage HTTP endpoint, Kafka, or a file).
No inbound ports or additional Kubernetes Services are required.

> **Dependency**: the Feast image must include `feast[openlineage]` (`openlineage-python`).

### HTTP transport (Marquez)

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample-openlineage
spec:
  feastProject: my_project
  openlineage:
    enabled: true
    transportType: http
    transportUrl: "http://marquez.feast.svc.cluster.local:5000"
    transportEndpoint: "api/v1/lineage"
    extraConfig:
      namespace: "my-feast-project"
      producer: "feast-operator"
      emit_on_apply: "true"
      emit_on_materialize: "true"
```

### HTTP with API key authentication

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: openlineage-secret
  namespace: feast
stringData:
  api_key: "<your-api-key>"
---
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample-openlineage-auth
  namespace: feast
spec:
  feastProject: my_project
  openlineage:
    enabled: true
    transportType: http
    transportUrl: "https://marquez.example.com"
    transportEndpoint: "api/v1/lineage"
    apiKeySecretRef:
      name: openlineage-secret    # Secret must contain key "api_key"
    extraConfig:
      namespace: "my-feast-project"
      emit_on_apply: "true"
      emit_on_materialize: "true"
```

The operator reads the `api_key` value from the Secret and writes it into
`feature_store.yaml`. The Secret must be in the same namespace as the `FeatureStore`.

### Kafka transport

```yaml
openlineage:
  enabled: true
  transportType: kafka
  extraConfig:
    namespace: "my-feast-project"
    emit_on_apply: "true"
    emit_on_materialize: "true"
    bootstrap_servers: "kafka.svc:9092"
    topic: "openlineage"
    sasl_mechanism: "PLAIN"
```

### Console transport (development)

Events are printed to stdout — useful for verifying integration without a backend:

```yaml
openlineage:
  enabled: true
  transportType: console
  extraConfig:
    emit_on_apply: "true"
    emit_on_materialize: "true"
```

### Field reference

| Field | Type | Description |
|-------|------|-------------|
| `enabled` | bool | Activates OpenLineage. Must be `true` |
| `transportType` | string | `http` / `console` / `file` / `kafka` |
| `transportUrl` | string | Base URL for HTTP transport |
| `transportEndpoint` | string | API path appended to `transportUrl` |
| `apiKeySecretRef.name` | string | Name of a Secret containing key `api_key` |
| `extraConfig` | map[string]string | Additional settings (see below) |

### `extraConfig` keys

Values that are `"true"` or `"false"` are automatically coerced to native YAML booleans
so that Feast's Pydantic `StrictBool` validators accept them.

| Key | Type | Description |
|-----|------|-------------|
| `namespace` | string | OpenLineage namespace for emitted events |
| `producer` | string | Producer identifier in emitted events |
| `emit_on_apply` | bool string | Emit events on `feast apply` |
| `emit_on_materialize` | bool string | Emit events on materialization |
| `bootstrap_servers` | string | Kafka: comma-separated broker addresses |
| `topic` | string | Kafka: target topic name |
| `sasl_mechanism` | string | Kafka: SASL mechanism (e.g. `PLAIN`, `SCRAM-SHA-256`) |
| `file_path` | string | File transport: path to write lineage events |

---

## Materialization (`spec.materialization`)

Controls how features are written to the online store during materialization jobs. Settings
are written into `feature_store.yaml` for all pods.

```yaml
spec:
  materialization:
    onlineWriteBatchSize: 10000
    extraConfig:
      pull_latest_features: "false"
```

### `onlineWriteBatchSize`

Limits the number of rows written per batch during materialization. Without this, all rows
for a feature view are written in a single batch — this can cause OOM for large feature views.

- Supported engines: **local**, **Spark**, **Ray**
- Minimum value: **1** (enforced by CRD validation)

```yaml
materialization:
  onlineWriteBatchSize: 10000    # 10 k rows per batch
```

### `extraConfig`

Passes additional `MaterializationConfig` settings inline. Boolean strings
(`"true"` / `"false"`) are coerced to native YAML booleans.

```yaml
materialization:
  extraConfig:
    pull_latest_features: "false"    # only materialize the latest value per entity
```

| Key | Type | Description |
|-----|------|-------------|
| `pull_latest_features` | bool string | When `"true"`, only the latest feature value per entity is materialized. Default is engine-dependent |

---

## Full example

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: openlineage-secret
  namespace: feast
stringData:
  api_key: "<your-api-key>"
---
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: feast-production
  namespace: feast
spec:
  feastProject: my_project
  materialization:
    onlineWriteBatchSize: 10000
  openlineage:
    enabled: true
    transportType: http
    transportUrl: "http://marquez.feast.svc.cluster.local:5000"
    transportEndpoint: "api/v1/lineage"
    apiKeySecretRef:
      name: openlineage-secret
    extraConfig:
      namespace: "my-feast-project"
      producer: "feast-operator"
      emit_on_apply: "true"
      emit_on_materialize: "true"
  services:
    onlineStore:
      server: {}
```

---

## See also

- [API reference — `OpenLineageConfig`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#openlineageconfig)
- [API reference — `MaterializationConfig`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#materializationconfig)
- [Sample: materialization + openlineage](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_materialization_openlineage.yaml)
- [Feast SDK — OpenLineage](../reference/openlineage.md)
- [Guide 6 — Batch Engine & Scheduled Jobs](06-batch-and-jobs.md)
