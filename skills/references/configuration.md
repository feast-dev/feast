# Configuration Reference

## Table of Contents
- [feature_store.yaml](#feature_storeyaml)
- [RepoConfig Fields](#repoconfig-fields)
- [Registry Configuration](#registry-configuration)
- [Online Store Types](#online-store-types)
- [Offline Store Types](#offline-store-types)
- [Batch Engine Types](#batch-engine-types)
- [Authentication](#authentication)
- [Feature Server](#feature-server)
- [Materialization Config](#materialization-config)
- [OpenLineage Config](#openlineage-config)
- [Feature Repository Layout](#feature-repository-layout)

## feature_store.yaml

Minimal local config:
```yaml
project: my_project
registry: data/registry.db
provider: local
online_store:
  type: sqlite
  path: data/online_store.db
```

GCP config:
```yaml
project: my_project
registry: gs://my-bucket/registry.pb
provider: gcp
online_store:
  type: datastore
offline_store:
  type: bigquery
```

AWS config:
```yaml
project: my_project
registry: s3://my-bucket/registry.pb
provider: aws
online_store:
  type: dynamodb
  region: us-east-1
offline_store:
  type: redshift
  cluster_id: my-cluster
  region: us-east-1
  database: feast
  user: admin
  s3_staging_location: s3://my-bucket/feast-staging
```

## RepoConfig Fields

| Field | Alias | Type | Default | Description |
|-------|-------|------|---------|-------------|
| `project` | - | str | required | Project namespace (alphanumeric + underscores) |
| `project_description` | - | str | None | Project description |
| `provider` | - | str | `"local"` | `"local"`, `"gcp"`, or `"aws"` |
| `registry` | `registry_config` | str/dict | required | Registry path or config object |
| `online_store` | `online_config` | str/dict | `"sqlite"` | Online store type or config |
| `offline_store` | `offline_config` | str/dict | `"dask"` | Offline store type or config |
| `batch_engine` | `batch_engine_config` | str/dict | `"local"` | Batch materialization engine |
| `auth` | - | dict | no_auth | Authentication config |
| `feature_server` | - | dict | None | Feature server config |
| `entity_key_serialization_version` | - | int | 3 | Entity key serialization version |
| `coerce_tz_aware` | - | bool | True | Coerce timestamps to timezone-aware |
| `materialization` | `materialization_config` | dict | default | Materialization options |
| `openlineage` | `openlineage_config` | dict | None | OpenLineage config |

## Registry Configuration

| Field | Default | Description |
|-------|---------|-------------|
| `registry_type` | `"file"` | `"file"`, `"sql"`, `"snowflake.registry"`, `"remote"` |
| `path` | `""` | Local path, GCS/S3 URI (file), or DB connection URL (sql) |
| `cache_ttl_seconds` | 600 | Registry cache TTL (0 = no expiry) |
| `cache_mode` | `"sync"` | `"sync"` or `"thread"` |
| `s3_additional_kwargs` | None | Extra boto3 kwargs for S3 |

### File registry
```yaml
registry: data/registry.db
```
or
```yaml
registry:
  registry_type: file
  path: data/registry.db
  cache_ttl_seconds: 60
```

### SQL registry
```yaml
registry:
  registry_type: sql
  path: postgresql://user:pass@host:5432/feast  # pragma: allowlist secret
  cache_ttl_seconds: 60
```

### Remote registry
```yaml
registry:
  registry_type: remote
  path: grpc://feast-registry-server:6570
```

## Online Store Types

| Type | Config Key | Use Case |
|------|-----------|----------|
| `sqlite` | `path` | Local development |
| `redis` | `connection_string` | Production, low-latency |
| `dynamodb` | `region` | AWS-native |
| `datastore` | `project_id` | GCP-native |
| `bigtable` | `project_id`, `instance` | GCP, high-throughput |
| `postgres` | `host`, `port`, `database`, `user`, `password` | Self-managed |
| `snowflake.online` | `account`, `database`, `schema` | Snowflake ecosystem |
| `milvus` | `host`, `port` | Vector search |
| `qdrant` | `host`, `port` | Vector search |
| `remote` | `path` | Remote feature server |

### Examples

```yaml
# SQLite (local dev)
online_store:
  type: sqlite
  path: data/online_store.db

# Redis
online_store:
  type: redis
  connection_string: redis://localhost:6379

# PostgreSQL
online_store:
  type: postgres
  host: localhost
  port: 5432
  database: feast
  db_schema: public
  user: postgres
  password: secret

# Milvus (vector search)
online_store:
  type: milvus
  host: localhost
  port: 19530
```

## Offline Store Types

| Type | Use Case |
|------|----------|
| `dask` | Local development (default) |
| `duckdb` | Local, fast analytics |
| `bigquery` | GCP |
| `snowflake.offline` | Snowflake |
| `redshift` | AWS |
| `spark` | Large-scale processing |
| `postgres` | Self-managed |
| `trino` | Federated queries |
| `athena` | AWS serverless |
| `clickhouse` | Analytics |
| `remote` | Remote offline server |

### Examples

```yaml
# DuckDB
offline_store:
  type: duckdb

# BigQuery
offline_store:
  type: bigquery
  project_id: my-gcp-project
  dataset: feast_dataset

# Snowflake
offline_store:
  type: snowflake.offline
  account: my_account
  user: user
  password: pass
  database: FEAST
  schema: PUBLIC
  warehouse: COMPUTE_WH

# Spark
offline_store:
  type: spark
  spark_conf:
    spark.master: "local[*]"
```

## Batch Engine Types

| Type | Description |
|------|-------------|
| `local` | Local Python process (default) |
| `snowflake.engine` | Snowflake-based materialization |
| `spark.engine` | Spark-based materialization |
| `lambda` | AWS Lambda-based |
| `k8s` | Kubernetes job-based |
| `ray.engine` | Ray-based |

```yaml
batch_engine:
  type: local
```

## Authentication

| Type | Description |
|------|-------------|
| `no_auth` | No authentication (default) |
| `kubernetes` | Kubernetes service account |
| `oidc` | OpenID Connect (server-side) |
| `oidc_client` | OpenID Connect (client-side) |

```yaml
# OIDC example
auth:
  type: oidc
  client_id: feast-client
  auth_server_url: https://auth.example.com
  auth_discovery_url: https://auth.example.com/.well-known/openid-configuration
```

## Feature Server

```yaml
feature_server:
  type: local
```

MCP-based feature server:
```yaml
feature_server:
  type: mcp
```

## Materialization Config

```yaml
materialization:
  pull_latest_features: false  # Only pull latest feature values per entity
```

## OpenLineage Config

```yaml
openlineage:
  enabled: true
  transport_type: http  # http, console, file, kafka
  transport_url: http://marquez:5000
  transport_endpoint: api/v1/lineage
  namespace: feast
  emit_on_apply: true
  emit_on_materialize: true
```

## Feature Repository Layout

```
my_feature_repo/
├── feature_store.yaml          # Required config
├── .feastignore                # Optional gitignore-style file
├── driver_features.py          # Feature definitions
├── customer_features.py        # More definitions
└── data/
    ├── driver_stats.parquet    # Data files (for FileSource)
    └── registry.db             # Auto-generated registry
```

- Feast recursively scans all `.py` files for feature definitions
- Use `.feastignore` to exclude files/directories from scanning
- `feast apply` registers all discovered definitions into the registry
