# Guide 2 — Persistence

The offline store, online store, and registry each need a place to store data. The operator
supports two persistence patterns for each:

- **File persistence** — a path on a volume (ephemeral or PVC-backed)
- **DB/store persistence** — an external database or managed store, wired via a Kubernetes Secret

The pattern is the same for all three services; only the store types differ.

---

## Persistence patterns at a glance

| Pattern | Best for | Data survives pod restart? |
|---------|----------|-----------------------------|
| **File — emptyDir** | Dev / CI | No |
| **File — PVC (ref)** | Single-node prod or testing | Yes (if PVC is retained) |
| **File — PVC (create)** | Operator-managed storage | Yes |
| **DB store** | Production, HA, multi-pod | Yes |

---

## File persistence

### Ephemeral (emptyDir)

The default when no `persistence` block is set. Data lives on the pod's local disk and is
lost on restart. Suitable for development only.

```yaml
services:
  onlineStore:
    server: {}          # no persistence block → emptyDir
```

### PVC — reference an existing PVC

When you already have a PVC provisioned (e.g. by your storage team):

```yaml
services:
  onlineStore:
    persistence:
      file:
        path: online_store.db          # filename relative to mountPath
        pvc:
          ref:
            name: online-pvc           # name of the existing PVC
          mountPath: /data/online      # where the PVC is mounted in the pod
```

### PVC — let the operator create one

```yaml
services:
  offlineStore:
    persistence:
      file:
        type: duckdb                   # offline file type: file | dask | duckdb
        pvc:
          create:
            storageClassName: standard
            resources:
              requests:
                storage: 20Gi
          mountPath: /data/offline
```

Omitting `storageClassName` uses the cluster default StorageClass. Omitting `create`
entirely creates a PVC with the operator's built-in defaults (1 Gi, default StorageClass).

```yaml
registry:
  local:
    persistence:
      file:
        path: registry.db
        pvc:
          create: {}                   # operator defaults: 1 Gi, default StorageClass
          mountPath: /data/registry
```

---

## DB / store persistence

For production, point the operator at an external database. The operator reads connection
details from a Kubernetes Secret and writes them into `feature_store.yaml`.

### Secret format

The Secret must contain one key per store component. The **key name** is the store type
(e.g. `postgres`, `sql`, `redis`). The **value** is a YAML snippet identical to what you
would write under the corresponding section in `feature_store.yaml`, **minus the `type:` key**
(the operator inserts it from `persistence.store.type`).

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-data-stores
stringData:
  # Key for online store type "postgres"
  postgres: |
    host: postgres.feast.svc.cluster.local
    port: 5432
    database: feast
    db_schema: public
    user: feast
    password: feast

  # Key for registry type "sql" (SQLAlchemy URL)
  sql: |
    path: postgresql+psycopg://feast:feast@postgres.feast.svc.cluster.local:5432/feast  #pragma: allowlist secret
    cache_ttl_seconds: 60
    sqlalchemy_config_kwargs:
      echo: false
      pool_pre_ping: true
```

Reference the Secret from the CR:

```yaml
services:
  onlineStore:
    persistence:
      store:
        type: postgres
        secretRef:
          name: feast-data-stores      # key "postgres" is read automatically
  registry:
    local:
      persistence:
        store:
          type: sql
          secretRef:
            name: feast-data-stores    # key "sql" is read automatically
```

> **Key lookup rule**: the operator looks up the Secret key that matches
> `persistence.store.type` (e.g. `type: postgres` → key `postgres`).
> Keep all stores in one Secret or split across multiple — both work.

### Injecting DB credentials into the server pod

The Secret key values in the example above hard-code passwords. For production, keep
credentials in a separate Secret and inject them as environment variables using `envFrom`,
then reference them with `${VAR}` substitution in the data-stores Secret value:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: postgres-creds
stringData:
  POSTGRES_USER: feast
  POSTGRES_PASSWORD: s3cr3t
---
apiVersion: v1
kind: Secret
metadata:
  name: feast-data-stores
stringData:
  postgres: |
    host: postgres.feast.svc.cluster.local
    port: 5432
    database: feast
    user: ${POSTGRES_USER}
    password: ${POSTGRES_PASSWORD}
```

```yaml
services:
  onlineStore:
    server:
      envFrom:
        - secretRef:
            name: postgres-creds      # expands ${POSTGRES_USER} / ${POSTGRES_PASSWORD}
    persistence:
      store:
        type: postgres
        secretRef:
          name: feast-data-stores
```

---

## Store types by component

### Online store

| `type` | Secret key | SDK docs |
|--------|------------|---------|
| `sqlite` | `sqlite` | [SQLite](../reference/online-stores/sqlite.md) |
| `redis` | `redis` | [Redis](../reference/online-stores/redis.md) |
| `postgres` | `postgres` | [Postgres](../reference/online-stores/postgres.md) |
| `cassandra` | `cassandra` | [Cassandra](../reference/online-stores/cassandra.md) |
| `hazelcast` | `hazelcast` | [Hazelcast](../reference/online-stores/hazelcast.md) |
| `hbase` | `hbase` | [HBase](../reference/online-stores/hbase.md) |
| `datastore` | `datastore` | [Datastore](../reference/online-stores/datastore.md) |
| `dynamodb` | `dynamodb` | [DynamoDB](../reference/online-stores/dynamodb.md) |
| `bigtable` | `bigtable` | [Bigtable](../reference/online-stores/bigtable.md) |

> For all store-specific YAML keys (connection options, pool sizes, etc.) see the
> linked SDK docs — the Secret value accepts the same keys.

### Offline store

| `type` (file) | Notes |
|-------|-------|
| `file` | Default pandas-based parquet offline store |
| `dask` | Dask-based parallel parquet |
| `duckdb` | DuckDB in-process analytical engine |

For external DB-backed offline stores (BigQuery, Snowflake, Spark, Trino, etc.), use
`persistence.store.type` and a Secret with the matching key. See
[Offline Stores](../reference/offline-stores/) in the SDK docs.

### Registry

| `type` | Secret key | Notes |
|--------|------------|-------|
| `file` | *(file persistence, no Secret)* | SQLite-backed file registry |
| `sql` | `sql` | SQLAlchemy URL — supports PostgreSQL, MySQL, SQLite |
| `snowflake.registry` | `snowflake.registry` | Snowflake-backed registry |

For `sql`, the Secret value is a `path:` (SQLAlchemy URL) plus optional `cache_ttl_seconds`
and `sqlalchemy_config_kwargs`:

```yaml
# feast-data-stores Secret, key "sql"
sql: |
  path: postgresql+psycopg://user:pass@host:5432/feast  #pragma: allowlist secret
  cache_ttl_seconds: 60
```

---

## Common patterns

### Redis online store

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-data-stores
stringData:
  redis: |
    connection_string: redis.feast.svc.cluster.local:6379
---
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample-redis
spec:
  feastProject: my_project
  services:
    onlineStore:
      server: {}
      persistence:
        store:
          type: redis
          secretRef:
            name: feast-data-stores
```

### Postgres for both online store and registry

```yaml
services:
  onlineStore:
    persistence:
      store:
        type: postgres
        secretRef:
          name: feast-data-stores     # reads key "postgres"
  offlineStore:
    persistence:
      file:
        type: duckdb
        pvc:
          create: {}
          mountPath: /data/offline
  registry:
    local:
      persistence:
        store:
          type: sql
          secretRef:
            name: feast-data-stores   # reads key "sql"
```

---

## See also

- [API reference — `OnlineStorePersistence`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#onlinestorepersistence)
- [API reference — `OfflineStorePersistence`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#offlinepersistence)
- [API reference — `RegistryPersistence`](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/docs/api/markdown/ref.md#registrypersistence)
- [Sample: DB persistence (Postgres)](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_db_persistence.yaml)
- [Sample: PVC persistence](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_pvc_persistence.yaml)
- [Sample: object store (GCS)](https://github.com/feast-dev/feast/blob/stable/infra/feast-operator/config/samples/v1_featurestore_gcs.yaml)
- [Feast SDK — Online Stores](../reference/online-stores/)
- [Feast SDK — Offline Stores](../reference/offline-stores/)
- [Feast SDK — Registries](../reference/registries/)
