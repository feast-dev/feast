---
name: feast-architecture
description: Internals of the Feast codebase — how each component works, where the key abstractions live, and the data flow through the system. Use when asked how feast apply works, how the registry stores data, how materialization moves data, how get_online_features retrieves features, how the feature server works, how the Kubernetes operator manages deployments, or when navigating the codebase to understand where to make a change.
license: Apache-2.0
compatibility: Works with Claude Code, OpenAI Codex, and any Agent Skills compatible tool.
metadata:
  author: feast-dev
  version: "1.0"
---

# Feast Architecture Internals

## Full Component Map

```
┌─────────────────────────────────────────────────────────────────┐
│                     Feast Deployment Modes                       │
│                                                                   │
│  Local / Python SDK          Kubernetes (feast-operator)          │
│  ─────────────────           ─────────────────────────────────   │
│  feature_store.yaml    ←──   FeatureStore CR (CRD)               │
│        │                            │                             │
│        ▼                            ▼                             │
│  FeatureStore (Python)      Operator deploys services:            │
│    ├── Registry               - feature-server (Go or Python)     │
│    ├── Provider               - offline-store-server              │
│    │   ├── OnlineStore        - registry-server                   │
│    │   └── OfflineStore     + manages feature_store.yaml config   │
│    └── FeatureServer                                              │
│        (Python FastAPI or                                         │
│         Go gRPC/HTTP)                                             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Python SDK Core

### FeatureStore — the orchestrator

**File**: `sdk/python/feast/feature_store.py`

`FeatureStore` is the single entry point for all operations. It never reads/writes data
directly — it delegates to the registry (for metadata) and the provider (for infrastructure
and data movement).

```python
FeatureStore(repo_path=".")   # loads feature_store.yaml
store.apply(objects)           # register feature definitions
store.materialize(...)         # offline → online
store.get_online_features(...) # serve
store.get_historical_features(...) # training data
```

**File**: `sdk/python/feast/repo_config.py` — parses `feature_store.yaml` into typed `RepoConfig`.
All component classes (online store, offline store, registry) are loaded dynamically from the
`type:` string via `repo_config.ONLINE_STORE_TYPE_MAP` / `OFFLINE_STORE_TYPE_MAP`.

---

### Registry

**Purpose**: Metadata store — persists definitions of entities, feature views, data sources,
feature services, permissions.

**Backends and their files:**

| Backend | File | Notes |
|---|---|---|
| File/GCS/S3 (default) | `infra/registry/registry.py` | Single proto blob, cached in memory |
| SQL | `infra/registry/sql.py` | Per-object tables via SQLAlchemy |
| Snowflake | `infra/registry/snowflake.py` | Snowflake tables |
| Remote | `infra/registry/remote.py` | Delegates to a remote registry server over gRPC |

**How the proto/file backend works:**
1. All metadata is serialized into one `Registry` protobuf (`protos/feast/core/Registry.proto`)
2. The proto blob is stored at the configured `registry:` path
3. In-memory `cached_registry_proto` is refreshed on a TTL (default 10s)
4. Writes re-serialize and overwrite the full blob — no partial updates

**Key pattern — apply:**
```python
# Python object → proto → stored in registry blob
registry.apply_feature_view(feature_view, project)
# → feature_view.to_proto()
# → upserts into cached_registry_proto.feature_views
# → registry_store.update_registry_proto(proto)
```

**Supporting files:**
- `infra/registry/base_registry.py` — abstract interface
- `infra/registry/proto_registry_utils.py` — proto serialization helpers
- `infra/registry/caching_registry.py` — adds TTL caching on top of any backend

---

### Provider

**Purpose**: Infrastructure lifecycle — creates/updates/tears down online store tables.
Also dispatches `online_write_batch` and `get_historical_features`.

**File**: `sdk/python/feast/infra/provider.py`

Built-in providers (set via `provider:` in `feature_store.yaml`):
- `local` — SQLite online store, file offline (dev default)
- `gcp` — Datastore/Bigtable online, BigQuery offline
- `aws` — DynamoDB online, Redshift offline

Custom providers extend `Provider` and override `update_infra` / `teardown_infra`.

---

### Online Store

**Purpose**: Low-latency feature serving. Stores the latest feature values per entity key.

**Interface**: `sdk/python/feast/infra/online_stores/online_store.py`
**Implementations**: `sdk/python/feast/infra/online_stores/` (redis, dynamodb, sqlite, bigtable, postgres, snowflake, …)

Key methods:
- `online_write_batch` — write entity→feature values
- `online_read` — read by entity keys
- `update` — provision/deprovision tables on `feast apply`
- `teardown` — clean up on `feast teardown`

---

### Offline Store

**Purpose**: Historical feature retrieval and training data generation (point-in-time joins).

**Interface**: `sdk/python/feast/infra/offline_stores/offline_store.py`
**Implementations**: `sdk/python/feast/infra/offline_stores/` (bigquery, snowflake, redshift, duckdb, file, …)

Returns a `RetrievalJob` (lazy) — no data moves until `.to_df()` or `.to_arrow()` is called.

PIT join logic (shared): `sdk/python/feast/infra/offline_stores/offline_utils.py`

**Key methods to implement for a new backend:**
```python
class MyOfflineStore(OfflineStore):
    def get_historical_features(self, config, feature_views, feature_refs,
                                entity_df, registry, project, ...) -> RetrievalJob: ...
    def pull_latest_from_table_or_query(self, config, data_source,
                                        join_key_columns, feature_name_columns,
                                        timestamp_field, created_timestamp_column,
                                        start_date, end_date) -> RetrievalJob: ...
    def pull_all_from_table_or_query(self, config, data_source, join_key_columns,
                                     feature_name_columns, timestamp_field,
                                     start_date, end_date) -> RetrievalJob: ...
    def write_logged_features(self, config, data, source, logging_config,
                              registry) -> None: ...  # optional
```

**Config class**: subclass `FeastConfigBaseModel` with a `type` Literal (short alias + full dotted path). Register in `OFFLINE_STORE_TYPE_MAP` in `sdk/python/feast/repo_config.py`.

**Data source**: each offline store backend pairs with a `DataSource` subclass (e.g. `BigQuerySource`, `FileSource`). Add it to `sdk/python/feast/data_sources/` and register in `DATA_SOURCE_CLASS_FOR_TYPE`.

---

## Data Flows

### `feast apply`
```
feast apply  (CLI → repo_operations.py)
  ├── Parse Python files → collect FeastObjects
  ├── store.apply(objects)
  │     ├── diff against registry (diff/registry_diff.py)
  │     ├── update registry metadata for changed objects
  │     └── provider.update_infra(tables_to_keep, tables_to_delete)
  │           └── online_store.update(...)  ← create/drop tables
  └── Write updated registry to storage
```

### `feast materialize`
```
store.materialize(start_date, end_date)
  ├── Load feature views from registry
  ├── For each feature view:
  │     ├── offline_store.pull_latest_from_table_or_query(...)
  │     │     └── Returns RetrievalJob (lazy)
  │     ├── job.to_arrow()  ← executes query, fetches Arrow table
  │     └── provider.online_write_batch(...)
  │           └── online_store.online_write_batch(config, table, data, progress)
  └── Update last_updated_timestamp in registry
```

### `get_online_features`
```
store.get_online_features(features, entity_rows)
  ├── Resolve feature refs → FeatureViews from registry
  ├── online_store.online_read(config, table, entity_rows, requested_features)
  │     └── Deserialize ValueProto → Python dict
  ├── Apply OnDemandFeatureView transformations (if any)
  └── Return OnlineFeaturesResponse
```

### `get_historical_features`
```
store.get_historical_features(entity_df, features)
  ├── Resolve feature refs → FeatureViews from registry
  ├── offline_store.get_historical_features(config, feature_views, entity_df)
  │     └── Point-in-time join:
  │           for each entity row, find latest values where
  │           event_timestamp ≤ entity_df.event_timestamp
  │           (prevents data leakage in training)
  └── Returns RetrievalJob → .to_df() / .to_arrow()
```

---

## Feature Servers

### Python Feature Server (FastAPI)

**File**: `sdk/python/feast/feature_server.py`

A FastAPI app that wraps `FeatureStore`. Started with `feast serve`.

Endpoints:
- `POST /get-online-features` — online feature retrieval
- `POST /push` — push features to online/offline store
- `POST /materialize` — trigger materialization
- `GET /health` — health check

The app loads `feature_store.yaml` at startup, creates a `FeatureStore`, and periodically
refreshes the registry in the background (async timer).

### Go Feature Server

**Directory**: `go/`
**Entry point**: `go/main.go`

A high-performance alternative to the Python feature server, written in Go.
Supports HTTP, HTTPS, and gRPC transports:

```bash
go run go/main.go -type http  -port 6566
go run go/main.go -type grpc  -port 6566
```

Key packages:
- `go/internal/feast/` — Go port of FeatureStore (reads feature_store.yaml, calls online store)
- `go/internal/feast/server/` — HTTP and gRPC server implementations
- `go/internal/feast/server/logging/` — feature logging to offline store

The Go server reads the registry directly (proto file or remote) and calls the online store.
It does not support `feast apply` or materialization — those remain Python-only.

---

## Feast Operator (Kubernetes)

**Directory**: `infra/feast-operator/`
**Language**: Go (controller-runtime / kubebuilder)

The operator manages the full lifecycle of a Feast deployment on Kubernetes via a
`FeatureStore` Custom Resource Definition (CRD).

### CRD: FeatureStore

**API version**: `feast.dev/v1`
**File**: `infra/feast-operator/api/v1/featurestore_types.go`

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: my-feast
spec:
  feastProjectName: my_project
  services:
    offlineStore:
      persistence:
        file:
          type: dask
    onlineStore:
      persistence:
        store:
          type: redis
          secretRef:
            name: redis-credentials
    registry:
      local:
        persistence:
          file:
            path: /data/registry.db
```

### What the operator manages

| Service | What it deploys |
|---|---|
| **Online Store server** | Deployment + Service for the feature server (Go or Python) |
| **Offline Store server** | Deployment + Service for the offline feature server |
| **Registry server** | Deployment + Service for the registry gRPC server |
| **feature_store.yaml** | ConfigMap auto-generated from the CR spec |
| **Materialization jobs** | CronJob (`spec.services.onlineStore.cronJob`) |
| **TLS** | Certificate management via `spec.services.*.tls` |
| **Auth** | OIDC / Kubernetes RBAC via `spec.authz` |

### Reconcile loop

**File**: `infra/feast-operator/internal/controller/featurestore_controller.go`

The `FeatureStoreReconciler.Reconcile` method runs on every CR change:
1. Fetches the `FeatureStore` CR
2. Calls `deployFeast()` → creates/updates Deployments, Services, ConfigMaps
3. Updates CR status conditions (`OfflineStore`, `OnlineStore`, `Registry` ready conditions)
4. Watches owned resources; re-reconciles on any change

Supporting services logic: `infra/feast-operator/internal/controller/services/`

---

## Serialization Layer

All persistent metadata and the feature server wire format use **Protocol Buffers**.

```
Python object (FeatureView, Entity, ...)
    ├── .to_proto()   → Protobuf message → stored in registry or sent over gRPC
    └── .from_proto() ← Protobuf message

Proto definitions:
  protos/feast/core/    # registry objects (FeatureView, Entity, DataSource, …)
  protos/feast/serving/ # serving API (GetOnlineFeaturesRequest/Response)
  protos/feast/types/   # Value, EntityKey, Field
```

When adding a new field to a Feast object:
1. Update the `.proto` file
2. Run `make compile-protos-python` (and `make compile-protos-go` if applicable)
3. Update `.to_proto()` and `.from_proto()` in the Python class

---

## Key Files Quick Reference

| Concern | Key file(s) |
|---|---|
| User-facing Python API | `sdk/python/feast/feature_store.py` |
| Config parsing | `sdk/python/feast/repo_config.py` |
| `feast apply` CLI logic | `sdk/python/feast/repo_operations.py` |
| Registry diff | `sdk/python/feast/diff/registry_diff.py` |
| Registry (proto/file) | `sdk/python/feast/infra/registry/registry.py` |
| Registry (SQL) | `sdk/python/feast/infra/registry/sql.py` |
| PIT join | `sdk/python/feast/infra/offline_stores/offline_utils.py` |
| Online store interface | `sdk/python/feast/infra/online_stores/online_store.py` |
| Entity key serialization | `sdk/python/feast/infra/online_stores/helpers.py` |
| Python feature server | `sdk/python/feast/feature_server.py` |
| Go feature server | `go/main.go`, `go/internal/feast/server/` |
| Operator CRD types | `infra/feast-operator/api/v1/featurestore_types.go` |
| Operator controller | `infra/feast-operator/internal/controller/featurestore_controller.go` |
| Operator services | `infra/feast-operator/internal/controller/services/` |
| Proto definitions | `protos/feast/` |
| Web UI | `ui/` (React) |

---

## Official Architecture Documentation

The `docs/` directory contains user-facing architecture documentation that complements this skill:

| Topic | Doc |
|---|---|
| Architecture overview | `docs/getting-started/architecture/overview.md` |
| Push vs pull model | `docs/getting-started/architecture/push-vs-pull-model.md` |
| Write patterns | `docs/getting-started/architecture/write-patterns.md` |
| Feature transformation | `docs/getting-started/architecture/feature-transformation.md` |
| RBAC / authorization | `docs/getting-started/architecture/rbac.md` |
| Online store component | `docs/getting-started/components/online-store.md` |
| Offline store component | `docs/getting-started/components/offline-store.md` |
| Registry component | `docs/getting-started/components/registry.md` |
| Feature server component | `docs/getting-started/components/feature-server.md` |
| Provider component | `docs/getting-started/components/provider.md` |
| Compute engine | `docs/getting-started/components/compute-engine.md` |
| ADRs (design decisions) | `docs/adr/` |
