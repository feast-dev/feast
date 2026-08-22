# Guide 4 — Registry Topology

The Feast registry stores metadata about feature views, entities, and feature services. The
operator supports three topology options:

| Topology | Use case |
|----------|----------|
| **Local** (file or DB) | Self-contained deployment; registry runs alongside other services |
| **Local + server** | Local registry exposed as a gRPC / REST server for remote clients |
| **Remote** | Multiple `FeatureStore` CRs share a single registry managed by another CR |

---

## Local registry

### File-backed (default)

```yaml
services:
  registry:
    local:
      persistence:
        file:
          path: registry.db
          pvc:
            create: {}
            mountPath: /data/registry
```

### DB-backed (SQL / Snowflake)

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-data-stores
stringData:
  sql: |
    path: postgresql+psycopg://feast:feast@postgres:5432/feast  #pragma: allowlist secret
    cache_ttl_seconds: 60
---
services:
  registry:
    local:
      persistence:
        store:
          type: sql
          secretRef:
            name: feast-data-stores   # reads key "sql"
```

### Exposing the registry as a server

Add `server: {}` under `registry.local` to expose it on port **6570**:

```yaml
services:
  registry:
    local:
      server: {}                       # exposes gRPC on 6570
      persistence:
        store:
          type: sql
          secretRef:
            name: feast-data-stores
```

The registry server also supports REST and gRPC independently:

```yaml
registry:
  local:
    server:
      restAPI: true    # enable REST API (default: true when server is set)
      grpc: true       # enable gRPC (default: true when server is set)
```

### MCP on the registry server

When the REST API is enabled, you can additionally expose registry metadata as
MCP (Model Context Protocol) tool endpoints for LLM agents:

```yaml
services:
  registry:
    local:
      server:
        restAPI: true
        mcp:
          enabled: true
      persistence:
        store:
          type: sql
          secretRef:
            name: feast-data-stores
```

The operator writes `registry.mcp.enabled: true` into `feature_store.yaml`.
A validation rule enforces that `restAPI` must be `true` when `mcp.enabled` is `true`.

See [Guide 3 — Serving & Observability](03-serving-and-observability.md#registry-mcp)
for more details and the full MCP configuration reference.

---

## Remote registry

A remote registry lets multiple `FeatureStore` CRs (e.g. in different namespaces or teams)
share a single registry. One CR owns the registry; the others point at it.

### Using a hostname

Point at any existing Feast registry server endpoint:

```yaml
services:
  registry:
    remote:
      hostname: feast-registry.feast-system.svc.cluster.local:6570
```

### Using `feastRef` (recommended for operator-managed registries)

`feastRef` lets one `FeatureStore` CR reference another CR's registry without hard-coding
hostnames. The operator resolves the Service name automatically:

```yaml
# CR that owns the registry (in namespace "feast-system")
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: registry-owner
  namespace: feast-system
spec:
  feastProject: shared_project
  services:
    registry:
      local:
        server: {}
        persistence:
          store:
            type: sql
            secretRef:
              name: feast-data-stores
```

```yaml
# CR that consumes the shared registry (in namespace "team-a")
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: team-a-store
  namespace: team-a
spec:
  feastProject: shared_project
  services:
    registry:
      remote:
        feastRef:
          name: registry-owner
          namespace: feast-system      # omit if same namespace
```

### Remote registry with TLS

If the remote registry server uses TLS, provide the CA certificate so clients can verify it:

```yaml
services:
  registry:
    remote:
      feastRef:
        name: registry-owner
        namespace: feast-system
      tls:
        configMapRef:
          name: registry-ca-cert       # ConfigMap containing the CA cert
        certKeyName: ca.crt            # key inside the ConfigMap (default: ca.crt)
```

---

## All-remote topology

For teams that deploy services independently, all services can use remote endpoints:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: client-store
spec:
  feastProject: my_project
  services:
    offlineStore:
      remote:
        hostname: feast-offline.feast-system.svc.cluster.local:8815
    onlineStore:
      remote:
        hostname: feast-online.feast-system.svc.cluster.local:6566
    registry:
      remote:
        feastRef:
          name: central-registry
          namespace: feast-system
```

---

## Client-side configuration (auto-generated ConfigMap)

When the operator deploys a `FeatureStore` CR, it automatically creates a ConfigMap named
`feast-<name>-client` in the same namespace. This ConfigMap contains a ready-to-use
`feature_store.yaml` that points at the deployed remote services (online store, offline
store, and registry).

For example, a CR named `testing` produces a ConfigMap `feast-testing-client`:

```yaml
# ConfigMap: feast-testing-client (auto-generated by the operator)
apiVersion: v1
kind: ConfigMap
metadata:
  name: feast-testing-client
data:
  feature_store.yaml: |
    project: testing
    provider: local
    online_store:
        path: https://feast-testing-online.feast.svc.cluster.local:443
        type: remote
        cert: /etc/pki/tls/custom-certs/ca-bundle.crt
    registry:
        path: feast-testing-registry.feast.svc.cluster.local:443
        registry_type: remote
        cert: /etc/pki/tls/custom-certs/ca-bundle.crt
    auth:
        type: no_auth
    entity_key_serialization_version: 3
```

### Using the client ConfigMap in-cluster

Mount the ConfigMap into your application pod so the Feast SDK discovers the configuration
automatically:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ml-serving-app
spec:
  template:
    spec:
      containers:
        - name: app
          volumeMounts:
            - name: feast-client-config
              mountPath: /opt/feast
              readOnly: true
      volumes:
        - name: feast-client-config
          configMap:
            name: feast-testing-client
```

Then initialize the Feast store from the mounted path:

```python
from feast import FeatureStore

store = FeatureStore(repo_path="/opt/feast")
```

### Using the client ConfigMap outside the cluster

Copy the ConfigMap content to your local `feature_store.yaml` for development or
testing outside the cluster. Adjust hostnames and TLS paths as needed (e.g. use
`kubectl port-forward` or an Ingress endpoint):

```sh
kubectl get configmap feast-testing-client -o jsonpath='{.data.feature_store\.yaml}' > feature_store.yaml
```

---

## See also

- [API reference — `RegistryConfig`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#registryconfig)
- [API reference — `LocalRegistryConfig`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#localregistryconfig)
- [API reference — `RemoteRegistryConfig`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#remoteregistryconfig)
- [Sample: all remote servers](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_all_remote_servers.yaml)
- [Sample: DB persistence](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_db_persistence.yaml)
- [Sample: MCP](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_mcp.yaml)
- [Feast SDK — Registries](../reference/registries/)
