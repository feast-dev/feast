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

## See also

- [API reference — `RegistryConfig`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#registryconfig)
- [API reference — `LocalRegistryConfig`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#localregistryconfig)
- [API reference — `RemoteRegistryConfig`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#remoteregistryconfig)
- [Sample: all remote servers](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_all_remote_servers.yaml)
- [Sample: DB persistence](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_db_persistence.yaml)
- [Feast SDK — Registries](../reference/registries/)
