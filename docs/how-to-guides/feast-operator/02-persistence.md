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

## Overriding the Secret key name

By default the operator looks up the Secret key that matches `persistence.store.type` (e.g.
`type: postgres` → key `postgres`). To use a different key, set `secretKeyName`:

```yaml
services:
  onlineStore:
    persistence:
      store:
        type: postgres
        secretRef:
          name: feast-data-stores
        secretKeyName: my_custom_key    # reads key "my_custom_key" instead of "postgres"
```

This is useful when a single Secret holds configuration for multiple stores of the same type,
or when you want a more descriptive key name.

---

## Validation rules

The operator enforces these rules on Secret values at reconciliation time:

1. The Secret key value must be **valid YAML** that deserializes to a map.
2. If the YAML contains a `type` field, its value **must match** the CR's `persistence.store.type`.
   Otherwise the operator rejects it with an error. Best practice: omit `type` from the Secret.
3. If the YAML contains a `registry_type` field (for registry stores), the same matching rule applies.
4. The Secret must exist in the **same namespace** as the FeatureStore CR.
5. Only **one** of `file` or `store` may be set under each persistence block (enforced by CRD validation).

---

## Complete Secret examples by store type

Below are copy-paste-ready Secret YAML snippets for every operator-supported store type.
Each snippet shows the Secret data key and the YAML value the operator expects.

> **Note**: omit the `type` field from Secret values — the operator injects it from
> `persistence.store.type`. Including a matching `type` value is tolerated but not
> recommended.

### Online store Secrets

#### Redis

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-online-store
stringData:
  redis: |
    connection_string: redis.feast.svc.cluster.local:6379
```

Redis Cluster with SSL:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-online-store
stringData:
  redis: |
    redis_type: redis_cluster
    connection_string: "redis1:6379,redis2:6379,ssl=true,password=my_password"
```

CR snippet:

```yaml
services:
  onlineStore:
    persistence:
      store:
        type: redis
        secretRef:
          name: feast-online-store
```

SDK reference: [Redis](../reference/online-stores/redis.md)

---

#### Postgres

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-online-store
stringData:
  postgres: |
    host: postgres.feast.svc.cluster.local
    port: 5432
    database: feast
    db_schema: public
    user: feast
    password: feast
```

With SSL:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-online-store
stringData:
  postgres: |
    host: postgres.feast.svc.cluster.local
    port: 5432
    database: feast
    db_schema: public
    user: feast
    password: feast
    sslmode: verify-ca
    sslrootcert_path: /path/to/server-ca.pem
```

CR snippet:

```yaml
services:
  onlineStore:
    persistence:
      store:
        type: postgres
        secretRef:
          name: feast-online-store
```

SDK reference: [Postgres](../reference/online-stores/postgres.md)

---

#### Cassandra

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-online-store
stringData:
  cassandra: |
    hosts:
      - 192.168.1.1
      - 192.168.1.2
      - 192.168.1.3
    keyspace: KeyspaceName
    port: 9042
    username: user
    password: secret
    protocol_version: 5
    load_balancing:
      local_dc: datacenter1
      load_balancing_policy: TokenAwarePolicy(DCAwareRoundRobinPolicy)
    read_concurrency: 100
    write_concurrency: 100
```

CR snippet:

```yaml
services:
  onlineStore:
    persistence:
      store:
        type: cassandra
        secretRef:
          name: feast-online-store
```

SDK reference: [Cassandra](../reference/online-stores/cassandra.md)

---

#### Snowflake (online)

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-online-store
stringData:
  snowflake.online: |
    account: snowflake_deployment.us-east-1
    user: user_login
    password: user_password
    role: SYSADMIN
    warehouse: COMPUTE_WH
    database: FEAST
```

CR snippet:

```yaml
services:
  onlineStore:
    persistence:
      store:
        type: snowflake.online
        secretRef:
          name: feast-online-store
```

SDK reference: [Snowflake](../reference/online-stores/snowflake.md)

---

#### DynamoDB

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-online-store
stringData:
  dynamodb: |
    region: us-west-2
    batch_size: 100
```

CR snippet:

```yaml
services:
  onlineStore:
    persistence:
      store:
        type: dynamodb
        secretRef:
          name: feast-online-store
```

SDK reference: [DynamoDB](../reference/online-stores/dynamodb.md)

---

#### Bigtable

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-online-store
stringData:
  bigtable: |
    project_id: my_gcp_project
    instance: my_bigtable_instance
```

CR snippet:

```yaml
services:
  onlineStore:
    persistence:
      store:
        type: bigtable
        secretRef:
          name: feast-online-store
```

SDK reference: [Bigtable](../reference/online-stores/bigtable.md)

---

#### Datastore

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-online-store
stringData:
  datastore: |
    project_id: my_gcp_project
    namespace: my_datastore_namespace
```

CR snippet:

```yaml
services:
  onlineStore:
    persistence:
      store:
        type: datastore
        secretRef:
          name: feast-online-store
```

SDK reference: [Datastore](../reference/online-stores/datastore.md)

---

#### MySQL

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-online-store
stringData:
  mysql: |
    host: mysql.feast.svc.cluster.local
    port: 3306
    database: feast
    user: feast
    password: feast
```

CR snippet:

```yaml
services:
  onlineStore:
    persistence:
      store:
        type: mysql
        secretRef:
          name: feast-online-store
```

SDK reference: [MySQL](../reference/online-stores/mysql.md)

---

#### Hazelcast

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-online-store
stringData:
  hazelcast: |
    cluster_name: dev
    cluster_members:
      - "localhost:5701"
    key_ttl_seconds: 36000
```

CR snippet:

```yaml
services:
  onlineStore:
    persistence:
      store:
        type: hazelcast
        secretRef:
          name: feast-online-store
```

SDK reference: [Hazelcast](../reference/online-stores/hazelcast.md)

---

#### HBase

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-online-store
stringData:
  hbase: |
    host: hbase-thrift.feast.svc.cluster.local
    port: "9090"
    connection_pool_size: 4
```

CR snippet:

```yaml
services:
  onlineStore:
    persistence:
      store:
        type: hbase
        secretRef:
          name: feast-online-store
```

SDK reference: [HBase](../reference/online-stores/hbase.md)

---

#### Other supported online store types

The following types also use the same pattern (`persistence.store.type` + `secretRef`).
Place the driver-specific YAML keys from the SDK docs under the matching Secret key:

| `type` | Secret key | SDK docs |
|--------|------------|----------|
| `sqlite` | `sqlite` | [SQLite](../reference/online-stores/sqlite.md) |
| `singlestore` | `singlestore` | [SingleStore](../reference/online-stores/singlestore.md) |
| `elasticsearch` | `elasticsearch` | [Elasticsearch](../reference/online-stores/elasticsearch.md) |
| `qdrant` | `qdrant` | [Qdrant](../reference/online-stores/qdrant.md) |
| `couchbase.online` | `couchbase.online` | [Couchbase](../reference/online-stores/couchbase.md) |
| `milvus` | `milvus` | [Milvus](../reference/online-stores/milvus.md) |
| `mongodb` | `mongodb` | [MongoDB](../reference/online-stores/mongodb.md) |
| `hybrid` | `hybrid` | [Hybrid](../reference/online-stores/hybrid.md) |

---

### Offline store Secrets

Offline DB stores follow the same pattern. The `type` field tells the operator which
store driver to use; the Secret value holds the connection parameters.

#### Snowflake (offline)

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-offline-store
stringData:
  snowflake.offline: |
    account: snowflake_deployment.us-east-1
    user: user_login
    password: user_password
    role: SYSADMIN
    warehouse: COMPUTE_WH
    database: FEAST
    schema: PUBLIC
```

CR snippet:

```yaml
services:
  offlineStore:
    persistence:
      store:
        type: snowflake.offline
        secretRef:
          name: feast-offline-store
```

SDK reference: [Snowflake](../reference/offline-stores/snowflake.md)

---

#### BigQuery

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-offline-store
stringData:
  bigquery: |
    dataset: feast_bq_dataset
    project_id: my_gcp_project
```

CR snippet:

```yaml
services:
  offlineStore:
    persistence:
      store:
        type: bigquery
        secretRef:
          name: feast-offline-store
```

SDK reference: [BigQuery](../reference/offline-stores/bigquery.md)

---

#### Postgres (offline)

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-offline-store
stringData:
  postgres: |
    host: postgres.feast.svc.cluster.local
    port: 5432
    database: feast
    db_schema: public
    user: feast
    password: feast
```

CR snippet:

```yaml
services:
  offlineStore:
    persistence:
      store:
        type: postgres
        secretRef:
          name: feast-offline-store
```

SDK reference: [Postgres](../reference/offline-stores/postgres.md)

---

#### Other supported offline store types

| `type` | Secret key | SDK docs |
|--------|------------|----------|
| `redshift` | `redshift` | [Redshift](../reference/offline-stores/redshift.md) |
| `spark` | `spark` | [Spark](../reference/offline-stores/spark.md) |
| `trino` | `trino` | [Trino](../reference/offline-stores/trino.md) |
| `athena` | `athena` | [Athena](../reference/offline-stores/athena.md) |
| `mssql` | `mssql` | [MSSQL](../reference/offline-stores/mssql.md) |
| `couchbase.offline` | `couchbase.offline` | [Couchbase](../reference/offline-stores/couchbase.md) |
| `clickhouse` | `clickhouse` | [ClickHouse](../reference/offline-stores/clickhouse.md) |
| `ray` | `ray` | [Ray](../reference/offline-stores/ray.md) |
| `oracle` | `oracle` | [Oracle](../reference/offline-stores/oracle.md) |

---

### Registry Secrets

#### SQL (SQLAlchemy) registry

The most common production registry. Uses a SQLAlchemy URL to connect to PostgreSQL,
MySQL, or SQLite:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-registry-store
stringData:
  sql: |
    path: postgresql+psycopg://feast:feast@postgres.feast.svc.cluster.local:5432/feast  #pragma: allowlist secret
    cache_ttl_seconds: 60
    sqlalchemy_config_kwargs:
      echo: false
      pool_pre_ping: true
```

CR snippet:

```yaml
services:
  registry:
    local:
      persistence:
        store:
          type: sql
          secretRef:
            name: feast-registry-store
```

SDK reference: [SQL Registry](../reference/registries/sql.md)

---

#### Snowflake registry

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-registry-store
stringData:
  snowflake.registry: |
    account: snowflake_deployment.us-east-1
    user: user_login
    password: user_password
    role: SYSADMIN
    warehouse: COMPUTE_WH
    database: FEAST
    schema: PUBLIC
    cache_ttl_seconds: 60
```

CR snippet:

```yaml
services:
  registry:
    local:
      persistence:
        store:
          type: snowflake.registry
          secretRef:
            name: feast-registry-store
```

SDK reference: [Snowflake Registry](../reference/registries/snowflake.md)

---

## Multi-store Secret (single Secret for all components)

You can combine all store configurations into a single Secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: feast-data-stores
stringData:
  redis: |
    connection_string: redis.feast.svc.cluster.local:6379
  snowflake.offline: |
    account: snowflake_deployment.us-east-1
    user: user_login
    password: user_password
    role: SYSADMIN
    warehouse: COMPUTE_WH
    database: FEAST
    schema: PUBLIC
  sql: |
    path: postgresql+psycopg://feast:feast@postgres.feast.svc.cluster.local:5432/feast  #pragma: allowlist secret
    cache_ttl_seconds: 60
---
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: production-store
spec:
  feastProject: my_project
  services:
    onlineStore:
      persistence:
        store:
          type: redis
          secretRef:
            name: feast-data-stores
    offlineStore:
      persistence:
        store:
          type: snowflake.offline
          secretRef:
            name: feast-data-stores
    registry:
      local:
        persistence:
          store:
            type: sql
            secretRef:
              name: feast-data-stores
```

---

## ConfigMap usage (batch engine)

The `batchEngine` is the only operator component that uses a **ConfigMap** rather than a
Secret for its configuration. The ConfigMap must contain a YAML value under key `config`
(default) or the key specified in `configMapKey`.

Unlike store Secrets, the batch engine ConfigMap value **must include the `type` field**:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: feast-batch-engine
data:
  config: |
    type: spark
    master: local
    spark_conf:
      spark.executor.memory: 4g
---
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample
spec:
  feastProject: my_project
  batchEngine:
    configMapRef:
      name: feast-batch-engine
    # configMapKey: config    # optional, defaults to "config"
```

See [Guide 6 — Batch & Jobs](06-batch-and-jobs.md) for full details.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `secret key X doesn't exist in secret Y` | The Secret key name doesn't match the store `type` | Either rename the Secret key to match `type`, or set `secretKeyName` in the CR |
| `secret X contains invalid value` | The Secret value is not valid YAML | Check indentation and quoting in the `stringData` value |
| `contains tag named type with value X` | The Secret includes a `type` field that doesn't match the CR's `persistence.store.type` | Remove `type` from the Secret value, or correct it to match |
| `invalid secret X for offline store` | The referenced Secret doesn't exist | Create the Secret in the same namespace as the FeatureStore CR |
| `One selection required between file or store` | Both `file` and `store` are set under a persistence block | Keep only one — choose either `file` persistence or `store` (DB) persistence |

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
