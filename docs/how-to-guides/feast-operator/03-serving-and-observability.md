# Guide 3 — Serving & Observability

This guide covers the `spec.services.onlineStore.serving` block and general server
configuration (`server` block) for all Feast services: per-worker tuning, log levels,
Prometheus metrics, offline push batching, and MCP (Model Context Protocol).

---

## Server configuration (`server` block)

Every deployable Feast service (online store, offline store, registry, UI) accepts a
`server` block under `spec.services.<service>`. The online and offline stores use
`ServerConfigs`; the registry uses `RegistryServerConfigs` (adds `restAPI` / `grpc` toggles).

### Enable a server

An empty `server: {}` is enough to deploy a service. Without it the component is not
deployed (e.g. the offline store runs as a local process, not as a network server):

```yaml
services:
  onlineStore:
    server: {}        # deploys the online feature server on port 6566
  offlineStore:
    server: {}        # deploys the offline feature server on port 8815
  registry:
    local:
      server: {}      # deploys the registry server on port 6570
  ui: {}              # deploys the Feast UI
```

### Log level

```yaml
services:
  onlineStore:
    server:
      logLevel: debug    # debug | info | warning | error | critical
  offlineStore:
    server:
      logLevel: info
  registry:
    local:
      server:
        logLevel: warning
```

### Container image and resources

The operator resolves the feature server image through the following priority chain:

1. **`server.image` in the CR** — per-service override, highest priority
2. **`RELATED_IMAGE_FEATURE_SERVER` env var on the operator pod** — cluster-wide default set by OLM/platform
3. **Built-in default** — `quay.io/feastdev/feature-server:<operator-version>`

The same `RELATED_IMAGE_FEATURE_SERVER` image is used for **all server containers** (online,
offline, registry, UI) and for the **init containers** (git clone, `feast apply`). Setting
it overrides all of them at once without touching any CR.

The **cronJob** container uses a separate env var: `RELATED_IMAGE_CRON_JOB`
(default: `quay.io/openshift/origin-cli:4.17`).

**Cluster-wide image override (operator env var)** — set this on the operator `Deployment`
to redirect all pods cluster-wide to a different registry (e.g. a private mirror or a
pinned digest):

```sh
kubectl set env deployment/feast-operator-controller-manager \
  RELATED_IMAGE_FEATURE_SERVER=my-registry.example.com/feast/feature-server:custom \
  RELATED_IMAGE_CRON_JOB=my-registry.example.com/tools/cli:latest \
  -n feast-operator-system
```

**Per-service CR override** — for a single `FeatureStore` or to pin one service to a
different image than the cluster default:

```yaml
services:
  onlineStore:
    server:
      image: quay.io/feastdev/feature-server:0.62.0
      resources:
        requests:
          cpu: "500m"
          memory: "512Mi"
        limits:
          cpu: "2"
          memory: "2Gi"
```

### Worker configuration (gunicorn)

The online and offline servers run on gunicorn. Tune worker count, connections, and
request limits:

```yaml
services:
  onlineStore:
    server:
      workerConfigs:
        workers: -1               # -1 = auto (2 × CPU cores + 1)
        workerConnections: 1000   # simultaneous clients per worker
        maxRequests: 1000         # requests before worker restart (memory leak prevention)
        maxRequestsJitter: 50     # jitter to avoid thundering herd
        keepAliveTimeout: 30      # keep-alive timeout in seconds
        registryTtlSec: 60        # registry refresh interval in seconds
```

> **Production recommendation**: set `workers: -1` and `registryTtlSec: 60` or higher.
> See [Online Server Performance Tuning](../online-server-performance-tuning.md)
> for detailed guidance.

### Environment variables and secrets

Inject environment variables from Secrets or ConfigMaps into any server:

```yaml
services:
  onlineStore:
    server:
      envFrom:
        - secretRef:
            name: my-db-credentials
      env:
        - name: FEAST_USAGE
          value: "false"
```

### Volume mounts

Mount additional volumes (ConfigMaps, Secrets, PVCs) into the server containers:

```yaml
services:
  volumes:
    - name: ca-cert
      configMap:
        name: cluster-ca
  onlineStore:
    server:
      volumeMounts:
        - name: ca-cert
          mountPath: /etc/ssl/certs/cluster-ca.crt
          subPath: ca.crt
```

---

## TLS

All servers support TLS termination. Provide a Kubernetes Secret containing the TLS
certificate and key, and reference it from `tls`:

```yaml
services:
  onlineStore:
    server:
      tls:
        secretRef:
          name: feast-tls       # Secret with keys tls.crt and tls.key
```

For mTLS (mutual TLS), also set a CA certificate ConfigMap:

```yaml
      tls:
        secretRef:
          name: feast-tls
        caCertConfigMapRef:
          name: cluster-ca
        certKeyName: tls.crt    # default
```

---

## Prometheus Metrics

When metrics are enabled the feature server starts an HTTP server on port **8000** for
Prometheus scraping. The operator automatically adds a `containerPort`, a Kubernetes `Service`
port, and a `ServiceMonitor` for Prometheus discovery.

### Two paths — use either or both

**Path 1: CLI flag** (existing, simple)

```yaml
services:
  onlineStore:
    server:
      metrics: true    # injects --metrics into feast serve; exposes port 8000
```

**Path 2: YAML config** (new, granular)

```yaml
services:
  onlineStore:
    serving:
      metrics:
        enabled: true
        categories:
          resource: true         # CPU / memory gauges
          request: true          # per-endpoint latency and request counters
          online_features: true  # feature retrieval metrics
          push: true             # push request counters
          materialization: true  # materialization counters and histograms
          freshness: false       # feature freshness (can be expensive at scale)
```

Both paths expose port 8000 and create a `ServiceMonitor`. When `serving.metrics.enabled`
is true the Python server reads it from `feature_store.yaml` directly; no `--metrics` flag
is injected. When `server.metrics: true` is used, the `--metrics` flag is injected.

> **SDK note**: `MetricsConfig` uses `extra="forbid"` in Pydantic. Only use category keys
> that are recognized by your Feast SDK version.

**Verify monitoring is wired:**
```sh
kubectl get servicemonitor -n <namespace>
kubectl port-forward svc/<name>-online 8000:8000 -n <namespace>
curl http://localhost:8000/metrics
```

---

## Offline Push Batching

When features are pushed to the online store via `/push`, each request also triggers a
synchronous offline store write. At high push throughput this causes OOM. Push batching
groups these writes into fixed-size batches flushed on a timer.

```yaml
services:
  onlineStore:
    serving:
      offlinePushBatching:
        enabled: true
        batchSize: 1000           # max rows per batch
        batchIntervalSeconds: 10  # flush interval in seconds
```

| Field | Type | Description |
|-------|------|-------------|
| `enabled` | bool | Activates batching |
| `batchSize` | int | Max rows per batch; flush when reached |
| `batchIntervalSeconds` | int | Flush interval even when batch is not full |

---

## MCP (Model Context Protocol)

MCP mounts LLM-agent-compatible tool endpoints alongside the existing REST API on port 6566.
The REST API is not replaced — MCP is additive.

The operator writes `feature_server.type: mcp` into `feature_store.yaml` only when
`serving.mcp.enabled: true`. Setting `enabled: false` reverts to `type: local`.

```yaml
services:
  onlineStore:
    server: {}
    serving:
      mcp:
        enabled: true
        serverName: feast-mcp-server
        serverVersion: "1.0.0"
        transport: sse           # "sse" (default) or "http"
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | bool | — | Must be `true`; `false` keeps `type: local` |
| `serverName` | string | `feast-mcp-server` | Name advertised to MCP clients |
| `serverVersion` | string | `1.0.0` | Version advertised to MCP clients |
| `transport` | string | `sse` | `sse` (SSE-based) or `http` (Streamable HTTP) |

MCP is mounted at `/mcp` on port 6566 — no additional Kubernetes Service is created.

> **Dependency**: the feature server image must include `feast[mcp]` (`fastapi-mcp`).
> Without it the server starts normally but MCP routes are not registered.

---

## `serving` vs `server` — summary

| Capability | `server.*` block | `serving.*` block |
|-----------|-----------------|------------------|
| Enable the server | `server: {}` | — |
| Log level | `server.logLevel` | — |
| Workers / gunicorn | `server.workerConfigs` | — |
| TLS | `server.tls` | — |
| Env vars / image | `server.env`, `server.image` | — |
| Metrics (simple) | `server.metrics: true` | — |
| Metrics (per-category) | — | `serving.metrics.categories` |
| Offline push batching | — | `serving.offlinePushBatching` |
| MCP | — | `serving.mcp` |

---

## See also

- [API reference — `ServerConfigs`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#serverconfigs)
- [API reference — `ServingConfig`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#servingconfig)
- [Sample: serving + metrics + offline push batching](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_serving.yaml)
- [Sample: MCP](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_mcp.yaml)
- [Sample: log levels](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_services_loglevel.yaml)
- [Feast SDK — Python Feature Server](../reference/feature-servers/python-feature-server.md)
- [Online Server Performance Tuning](../online-server-performance-tuning.md)
