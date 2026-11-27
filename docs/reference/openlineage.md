# OpenLineage Integration

Feast provides **native integration** with [OpenLineage](https://openlineage.io/), enabling automated data lineage tracking across ML feature engineering workflows. Feast supports both **producer** and **consumer** roles — emitting lineage events for its own operations and receiving lineage from any OpenLineage-compatible system for unified visualization.

## Quick Start

### 1. Install

```bash
pip install feast[openlineage]
# or: pip install openlineage-python
```

### 2. Configure

```yaml
# feature_store.yaml
project: my_project
registry:
  registry_type: sql
  path: sqlite:///data/registry.db
provider: local
online_store:
  type: sqlite
  path: data/online_store.db

openlineage:
  enabled: true
  transport_type: console   # or http, file, kafka
  namespace: my_project
  consumer:
    enabled: true
    store_type: sql
```

### 3. Apply and View

```bash
feast apply        # emits lineage events automatically
feast ui           # starts the UI with lineage visualization
feast serve_lineage  # starts standalone lineage server (optional)
```

Open http://localhost:8888 and navigate to the **Lineage** tab to view the full lineage graph, including both the Feast registry view and the OpenLineage view.

## Overview

When enabled, the integration **automatically** emits OpenLineage events for:

- **Registry changes** — events when feature views, on-demand feature views, feature services, entities, data sources, and saved datasets are applied
- **Feature materialization** — START, COMPLETE, and FAIL events when features are materialized

**No code changes required** — enable OpenLineage in your `feature_store.yaml` to begin tracking lineage automatically.

## Prerequisites

- **SQL registry required for consumer**: The OpenLineage consumer stores lineage data in SQL tables. Enabling the consumer requires the Feast registry to use `registry_type: sql` (SQLite, PostgreSQL, or MySQL). File-based registries are not supported for the consumer. The producer operates with any registry type.

## Producer Configuration

Add the `openlineage` section to your `feature_store.yaml`:

```yaml
openlineage:
  enabled: true
  transport_type: http
  transport_url: http://localhost:6580
  transport_endpoint: api/v1/lineage
  namespace: my_project
  emit_on_apply: true
  emit_on_materialize: true
```

### Environment Variables

You can also configure via environment variables:

```bash
export FEAST_OPENLINEAGE_ENABLED=true
export FEAST_OPENLINEAGE_TRANSPORT_TYPE=http
export FEAST_OPENLINEAGE_URL=http://localhost:6580
export FEAST_OPENLINEAGE_ENDPOINT=api/v1/lineage
export FEAST_OPENLINEAGE_NAMESPACE=my_project
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `enabled` | `false` | Enable/disable OpenLineage integration |
| `transport_type` | `None` | Transport type: `http`, `console`, `file`, `kafka`. When unset, defers to OpenLineage SDK defaults |
| `transport_url` | — | Base URL for HTTP transport (required when `transport_type` is `http`) |
| `transport_endpoint` | `api/v1/lineage` | API endpoint appended to `transport_url` for HTTP transport |
| `api_key` | — | Optional API key for authentication with the lineage server |
| `namespace` | `feast` | Namespace for lineage events. When set to `feast` (default), the Feast project name is used |
| `producer` | `feast` | Producer identifier included in every OpenLineage event |
| `emit_on_apply` | `true` | Emit lineage events when `feast apply` is called |
| `emit_on_materialize` | `true` | Emit lineage events during materialization |
| `additional_config` | `{}` | Extra transport-specific settings (e.g., `log_file_path` for file transport, `bootstrap_servers` for Kafka) |

### Namespace Behavior

- If `namespace` is `"feast"` (default): uses the project name as the namespace (e.g., `my_project`)
- If `namespace` is set to a custom value: uses `{namespace}/{project}` (e.g., `custom/my_project`)

## Usage

Once configured, lineage is tracked automatically:

```python
from feast import FeatureStore
from datetime import datetime, timedelta

fs = FeatureStore(repo_path="feature_repo")

# Apply operations emit lineage events automatically
fs.apply([driver_entity, driver_hourly_stats_view])

# Materialize emits START, COMPLETE/FAIL events automatically
fs.materialize(
    start_date=datetime.now() - timedelta(days=1),
    end_date=datetime.now()
)
```

## Lineage Graph Structure

When you run `feast apply`, Feast creates lineage events reflecting the full dependency graph:

```
DataSource ──────────┐
                     ├──→ FeatureView ──────────────┐
Entity ──────────────┘        │                     │
                              │                     ├──→ FeatureService
                              ▼                     │
RequestSource ──→ OnDemandFeatureView ──────────────┘
                              │
FeatureView ─────────────────┘ (as input source)

FeatureService ──→ SavedDataset
DataSource ──────→ SavedDataset (via storage matching)
```

**Jobs created per `feast apply`:**

| Job | Inputs | Outputs |
|-----|--------|---------|
| `feast_apply_entities` | — | Entity datasets |
| `feast_apply_data_sources` | — | DataSource datasets |
| `feast_apply_feature_view_{name}` | DataSource + Entity | FeatureView dataset |
| `feast_apply_odfv_{name}` | FeatureView + RequestSource | OnDemandFeatureView dataset |
| `feast_apply_feature_service_{name}` | FeatureView(s) + ODFV(s) | FeatureService dataset |
| `feast_apply_saved_dataset_{name}` | FeatureService + DataSource | SavedDataset dataset |

**Datasets include:**

- OpenLineage `SchemaDatasetFacet` with feature names, types, and descriptions
- Feast-specific facets with rich metadata (TTL, entities, owner, tags, etc.)

## Feast to OpenLineage Mapping

| Feast Concept | OpenLineage Concept | Facet |
|---------------|---------------------|-------|
| DataSource | InputDataset | `FeastDataSourceFacet` |
| Entity | InputDataset | `FeastEntityFacet` |
| FeatureView | OutputDataset (of FV job) / InputDataset (of FS or ODFV job) | `FeastFeatureViewFacet` |
| OnDemandFeatureView | OutputDataset | `FeastFeatureViewFacet` (with `mode: ON_DEMAND`) |
| StreamFeatureView | OutputDataset | `FeastFeatureViewFacet` (with `mode: STREAM`) |
| FeatureService | OutputDataset | `FeastFeatureServiceFacet` |
| SavedDataset | OutputDataset | `FeastSavedDatasetFacet` |
| Feature | Schema field in `SchemaDatasetFacet` | — |
| Materialization | RunEvent (START/COMPLETE/FAIL) | `FeastMaterializationFacet` |
| Online Store (per FV) | OutputDataset (materialization target) | `FeastOnlineStoreFacet` |

## Custom Feast Facets

The integration includes custom OpenLineage facets that carry Feast-specific metadata:

### FeastFeatureViewFacet

Captures metadata about feature views (regular, on-demand, and stream):

| Field | Description |
|-------|-------------|
| `name` | Feature view name |
| `ttl_seconds` | Time-to-live in seconds (0 = no TTL) |
| `entities` | List of entity names |
| `features` | List of feature names |
| `online_enabled` / `offline_enabled` | Store configuration |
| `mode` | Transformation mode: `ON_DEMAND`, `STREAM`, `PYTHON`, `PANDAS`, etc. |
| `description` | Human-readable description |
| `owner` | Owner identifier |
| `tags` | Key-value tags |

### FeastFeatureServiceFacet

Captures metadata about feature services:

| Field | Description |
|-------|-------------|
| `name` | Feature service name |
| `feature_views` | List of feature view names |
| `feature_count` | Total number of features |
| `description` | Description |
| `owner` | Owner identifier |
| `tags` | Key-value tags |
| `logging_enabled` | Whether feature logging is enabled |

### FeastDataSourceFacet

Captures metadata about data sources:

| Field | Description |
|-------|-------------|
| `name` | Data source name |
| `source_type` | Type: `FileSource`, `BigQuerySource`, `SnowflakeSource`, `RequestSource`, etc. |
| `timestamp_field` | Event timestamp column name |
| `created_timestamp_field` | Created timestamp column name |
| `field_mapping` | Source-to-feature field mapping |
| `description` | Description |
| `tags` | Key-value tags |

### FeastEntityFacet

Captures metadata about entities (join keys for feature lookups):

| Field | Description |
|-------|-------------|
| `name` | Entity name |
| `join_keys` | List of join key column names |
| `value_type` | Data type (INT64, STRING, etc.) |
| `description` | Description |
| `owner` | Owner identifier |
| `tags` | Key-value tags |

### FeastSavedDatasetFacet

Captures metadata about saved datasets (materialized feature snapshots):

| Field | Description |
|-------|-------------|
| `name` | Saved dataset name |
| `features` | List of feature names |
| `join_keys` | List of join key column names |
| `feature_service_name` | Name of the FeatureService that produced this dataset |
| `full_feature_names` | Whether full feature names were used |
| `description` | Description |
| `tags` | Key-value tags |

### FeastMaterializationFacet

Captures materialization run metadata (attached to RunEvents):

| Field | Description |
|-------|-------------|
| `feature_views` | Feature views being materialized |
| `start_date` / `end_date` | Materialization time window |
| `project` | Feast project name |
| `rows_written` | Number of rows written |
| `online_store_type` | Online store backend type |
| `offline_store_type` | Offline store backend type |

### FeastOnlineStoreFacet

Identifies the online store sink during materialization:

| Field | Description |
|-------|-------------|
| `feature_view` | Feature view whose features are stored |
| `store_type` | Online store backend (redis, sqlite, dynamodb, etc.) |
| `description` | Description |

### FeastProjectFacet

Captures Feast project context on job events:

| Field | Description |
|-------|-------------|
| `project_name` | Feast project name |
| `provider` | Infrastructure provider (local, gcp, aws) |
| `online_store_type` | Online store type |
| `offline_store_type` | Offline store type |
| `registry_type` | Registry type (file, sql) |

### FeastJobKindFacet

Distinguishes Feast jobs by semantic role:

| Field | Description |
|-------|-------------|
| `kind` | `definition` (registry/apply events) or `transform` (runtime materialize/compute) |
| `feast_project` | Feast project name |

### FeastRetrievalFacet

Captures feature retrieval metadata:

| Field | Description |
|-------|-------------|
| `retrieval_type` | `online` or `historical` |
| `feature_service` | Feature service name (if used) |
| `feature_views` | Feature views queried |
| `features` | Features retrieved |
| `entity_count` | Number of entities queried |
| `full_feature_names` | Whether full feature names were used |

## Transport Types

### HTTP Transport (Recommended for Production)

```yaml
openlineage:
  enabled: true
  transport_type: http
  transport_url: http://feast-example-lineage:6580
  transport_endpoint: api/v1/lineage
  api_key: your-api-key
```

### Console Transport (Development)

```yaml
openlineage:
  enabled: true
  transport_type: console
```

Events are printed to stdout. This transport is recommended for development and troubleshooting purposes.

### File Transport

```yaml
openlineage:
  enabled: true
  transport_type: file
  additional_config:
    log_file_path: openlineage_events.json
```

### Kafka Transport

```yaml
openlineage:
  enabled: true
  transport_type: kafka
  additional_config:
    bootstrap_servers: localhost:9092
    topic: openlineage.events
```

## Lineage Visualization

### Option 1: Feast UI (Built-in Consumer)

Feast includes a built-in OpenLineage consumer that receives, stores, and visualizes lineage from **all** OpenLineage producers directly in the Feast UI. See the [OpenLineage Consumer](#openlineage-consumer) section below.

### Option 2: Marquez

Use [Marquez](https://marquezproject.ai/) to visualize your Feast lineage:

```bash
docker run -p 5000:5000 -p 3000:3000 marquezproject/marquez
```

Configure Feast to emit to Marquez:

```yaml
openlineage:
  enabled: true
  transport_type: http
  transport_url: http://localhost:5000
```

Access the Marquez UI at http://localhost:3000.

---

## OpenLineage Consumer

Feast can act as an **OpenLineage consumer**, receiving lineage events from any OpenLineage-compatible producer and displaying them in the Feast UI. This provides a unified lineage experience without requiring a separate metadata platform such as Marquez.

### Consumer Architecture

```
Producers (Airflow, Spark, dbt, Feast, Flink, …)
            │
            ▼
   POST /api/v1/lineage  ──→  Event Processor ──→  Lineage Store (SQL)
                                                          │
                                                          ▼
                                                    Feast UI
                                          ┌──────────────────────────┐
                                          │  Lineage tab             │
                                          │  ├─ OpenLineage Graph    │
                                          │  │   (all producers)     │
                                          │  └─ ☐ Feast Only Lineage │
                                          │      (registry view)     │
                                          │                          │
                                          │  Events tab              │
                                          │  └─ Event browser        │
                                          └──────────────────────────┘
```

When the consumer is **not** enabled, the Feast UI shows only the original registry-based lineage view.

### Enabling the Consumer

Add the `consumer` section under `openlineage` in your `feature_store.yaml`:

```yaml
project: my_project
registry:
  registry_type: sql                                      # Required for consumer
  path: postgresql://user:****@host:5432/feast            # pragma: allowlist secret

openlineage:
  enabled: true
  namespace: my_project
  consumer:
    enabled: true
    store_type: sql
    # Optional: separate database for lineage storage.
    # If omitted, the SQL registry database is reused.
    # connection_string: postgresql://user:****@host:5432/feast_lineage
    api_key: "change-me"                                  # pragma: allowlist secret
    namespace_mapping:
      "spark://ml-team": "my_project"
      "airflow://prod-cluster": "my_project"
```

Or via environment variables:

```bash
export FEAST_OPENLINEAGE_CONSUMER_ENABLED=true
export FEAST_OPENLINEAGE_CONSUMER_STORE_TYPE=sql
export FEAST_OPENLINEAGE_CONSUMER_API_KEY=change-me                     # pragma: allowlist secret
# Optional separate DB:
# export FEAST_OPENLINEAGE_CONSUMER_CONNECTION_STRING=postgresql://...
# Namespace mapping (JSON format):
export FEAST_OPENLINEAGE_CONSUMER_NAMESPACE_MAPPING='{"spark://ml-team": "my_project", "airflow://prod-cluster": "my_project"}'
```

### Consumer Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `consumer.enabled` | `false` | Enable the OpenLineage consumer |
| `consumer.store_type` | `sql` | Storage backend type. Currently only `sql` is supported |
| `consumer.connection_string` | — | Separate database connection string for lineage storage. If omitted, the SQL registry database is used |
| `consumer.api_key` | — | API key required by producers when submitting events |
| `consumer.namespace_mapping` | `{}` | Maps external OpenLineage namespaces to Feast project names for RBAC scoping (see [Namespace Mapping](#namespace-mapping)) |
| `consumer.retention_days` | `30` | Number of days to retain events and runs. Set to `0` to disable automatic pruning |
| `consumer.retention_check_interval_hours` | `6` | Interval (in hours) between background pruning cycles |
| `consumer.standalone_server` | `false` | When `true`, the retention background task is delegated to the standalone lineage server. All consumer API endpoints remain available on both servers |

### Event Retention

The consumer includes an automatic retention policy that prunes expired events and runs to prevent unbounded storage growth. By default, data older than **30 days** is removed every **6 hours**.

**Pruned data:** Events (`openlineage_events`) and runs (`openlineage_runs`, `openlineage_run_io`) exceeding the configured retention period.

**Preserved data:** The current-state graph (jobs, datasets, edges, symlinks) is never pruned. These tables represent the latest lineage topology and are independent of historical event data.

```yaml
openlineage:
  consumer:
    enabled: true
    retention_days: 7          # Keep only 7 days of events
    retention_check_interval_hours: 1   # Check every hour
```

To disable automatic pruning:

```yaml
openlineage:
  consumer:
    retention_days: 0          # Keep everything
```

**Environment variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `FEAST_OPENLINEAGE_CONSUMER_RETENTION_DAYS` | `30` | Retention period in days |
| `FEAST_OPENLINEAGE_CONSUMER_RETENTION_CHECK_INTERVAL_HOURS` | `6` | Pruning check interval in hours |

**API endpoints:**

- `GET /api/v1/lineage/openlineage/retention` — returns current retention config and storage stats (row counts, oldest timestamps)
- `POST /api/v1/lineage/openlineage/retention/prune` — manually trigger pruning (requires API key)

### Running the Server

The `feast ui` command starts a unified server that provides:

- The Feast UI with integrated lineage visualization
- OpenLineage consumer endpoints for both event ingestion and lineage queries
- Access to the Feast registry

```bash
feast ui --port 8888
```

When both producer and consumer are enabled, Feast events generated by `feast apply` and materialization operations are **automatically ingested** into the local consumer store through an internal event pipeline. No additional HTTP transport configuration is required for self-reporting.

```yaml
# Minimal config for producer + consumer (self-contained)
openlineage:
  enabled: true
  transport_type: console   # optional: also prints events to stdout for verification
  namespace: my_project
  consumer:
    enabled: true
```

### Consumer API Endpoints

When the consumer is enabled, the following endpoints are available. All paths shown are relative to the server mount point (e.g., `/api/v1` on the UI server).

#### Event Ingestion (Producer-facing)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/lineage` | `POST` | Receive a single OpenLineage event (or array of events). Returns `201` for single events, `200` for batch |
| `/api/v1/lineage/batch` | `POST` | Receive a batch of OpenLineage events. Returns `204` on full success |

Both endpoints accept the `X-API-Key` header (or `Authorization: Bearer <key>`) when `consumer.api_key` is configured.

#### Lineage Query Endpoints (UI-facing)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/lineage/openlineage/graph` | `GET` | Full lineage graph with nodes, edges, and symlinks. Supports `?namespace=X`, `?limit=N`, `?offset=N` |
| `/api/v1/lineage/openlineage/graph/{node_type}/{namespace}/{name}` | `GET` | Lineage subgraph centered on a specific node. Supports `?depth=N`, `?direction=both|upstream|downstream` |
| `/api/v1/lineage/openlineage/namespaces` | `GET` | List all distinct namespaces |
| `/api/v1/lineage/openlineage/events` | `GET` | Browse events with `?namespace=X`, `?job_name=Y`, `?limit=N`, `?offset=N` |
| `/api/v1/lineage/openlineage/jobs` | `GET` | List all known jobs |
| `/api/v1/lineage/openlineage/datasets` | `GET` | List all known datasets |
| `/api/v1/lineage/openlineage/runs` | `GET` | List runs with `?job_namespace=X&job_name=Y`, `?limit=N`, `?offset=N` |
| `/api/v1/lineage/openlineage/runs/{run_id}` | `GET` | Single run detail with input/output datasets |

#### Registry Lineage Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/lineage/registry` | `GET` | Feast registry lineage with `?project=X` |
| `/api/v1/lineage/registry/all` | `GET` | Registry lineage for all projects |
| `/api/v1/lineage/objects/{object_type}/{object_name}` | `GET` | Detail for a specific registry object |
| `/api/v1/lineage/complete` | `GET` | Complete registry lineage with full object metadata |
| `/api/v1/lineage/complete/all` | `GET` | Complete registry lineage for all projects |

#### Admin Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/lineage/openlineage/reset` | `DELETE` | Purge all OpenLineage data. Accepts `?namespace=X` to delete a specific namespace only. Requires API key |

### Configuring External Producers

Any OpenLineage-compatible producer can be configured to send events to the Feast consumer. The standard ingestion endpoint is `POST /api/v1/lineage`.

#### Airflow

```python
# In airflow.cfg or environment
OPENLINEAGE_URL = "http://feast-server:8888"
OPENLINEAGE_ENDPOINT = "api/v1/lineage"
OPENLINEAGE_API_KEY = "change-me"                                       # pragma: allowlist secret
```

#### Spark

```properties
spark.openlineage.transport.type=http
spark.openlineage.transport.url=http://feast-server:8888
spark.openlineage.transport.endpoint=api/v1/lineage
spark.openlineage.transport.auth.type=api_key
spark.openlineage.transport.auth.apiKey=change-me
```

#### dbt

```yaml
# In profiles.yml or environment
OPENLINEAGE_URL: "http://feast-server:8888"
OPENLINEAGE_ENDPOINT: "api/v1/lineage"
OPENLINEAGE_API_KEY: "change-me"                                        # pragma: allowlist secret
```

#### Feast (Self-reporting)

When both the OpenLineage producer and consumer are enabled in the same `feature_store.yaml`, Feast events generated by `feast apply` and materialization operations are automatically ingested into the local consumer store through an internal event pipeline. No additional HTTP transport configuration is required.

```yaml
openlineage:
  enabled: true
  namespace: my_project
  consumer:
    enabled: true
    api_key: change-me                                                  # pragma: allowlist secret
```

### Producer Discovery (Kubernetes)

In a Kubernetes cluster with multiple OpenLineage producers (Spark, Airflow, dbt, and others),
each producer requires the consumer endpoint URL. The Feast Operator automates this by
creating a centralized **discovery ConfigMap** that producers can mount or reference.

#### Automatic Discovery ConfigMap

When a `FeatureStore` CR has an enabled OpenLineage consumer, the operator creates a
ConfigMap named `feast-openlineage-config` in the **controller namespace**
(e.g., `feast-operator-system` on plain Kubernetes, or the operator's namespace on
OpenShift). This ConfigMap is readable by all authenticated users via a Role + RoleBinding.

The ConfigMap contains three data keys:

| Key               | Format | Description |
|-------------------|--------|-------------|
| `openlineage.yml` | YAML   | Ready-to-mount OpenLineage client config with transport URL and endpoint |
| `url`             | String | Plain consumer base URL for simple env-var injection |
| `endpoints`       | JSON   | Registry of all Feast instances with enabled consumers (for multi-consumer clusters) |

**Example ConfigMap content:**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: feast-openlineage-config
  namespace: feast-operator-system
data:
  openlineage.yml: |
    transport:
      type: http
      url: http://feast-sample-lineage.feast-ns.svc.cluster.local:6580
      endpoint: api/v1/lineage
  url: http://feast-sample-lineage.feast-ns.svc.cluster.local:6580
  endpoints: '{"consumers":{"feast-ns/feast-sample":{"url":"http://feast-sample-lineage.feast-ns.svc.cluster.local:6580","endpoint":"api/v1/lineage","tls":false}}}'
```

#### Method 1: Mount as `openlineage.yml` (Recommended)

Mount the ConfigMap as a file and set the `OPENLINEAGE_CONFIG` environment variable.
This works with all OpenLineage producers (Python, Java, Spark, Airflow, dbt):

```yaml
# In producer Pod spec
spec:
  containers:
    - name: spark-driver
      env:
        - name: OPENLINEAGE_CONFIG
          value: /etc/openlineage/openlineage.yml
      volumeMounts:
        - name: ol-config
          mountPath: /etc/openlineage
          readOnly: true
  volumes:
    - name: ol-config
      configMap:
        name: feast-openlineage-config
        items:
          - key: openlineage.yml
            path: openlineage.yml
```

#### Method 2: Environment Variable Injection

For simpler setups, inject the URL directly from the ConfigMap:

```yaml
# In producer Pod spec
env:
  - name: OPENLINEAGE_URL
    valueFrom:
      configMapKeyRef:
        name: feast-openlineage-config
        key: url
        optional: true
```

If API key authentication is enabled, also inject:

```yaml
  - name: OPENLINEAGE_API_KEY
    valueFrom:
      secretKeyRef:
        name: my-openlineage-api-key
        key: api_key
```

#### Method 3: FeatureStore CR Status

The lineage server hostname is exposed in the `FeatureStore` CR status:

```bash
kubectl get featurestore my-feast -o jsonpath='{.status.serviceHostnames.lineage}'
# feast-my-feast-lineage.feast-ns.svc.cluster.local:80
```

This approach is suitable for scripts and automation workflows that interact with the Kubernetes API.

#### Multi-Consumer Clusters

When multiple Feast instances have OpenLineage consumers enabled, the `endpoints` key in the
ConfigMap contains a registry of all available consumers. Producers can parse this JSON
to select the appropriate consumer based on the Feast instance namespace or name:

```json
{
  "consumers": {
    "team-a/feast-a": {
      "url": "http://feast-feast-a-lineage.team-a.svc.cluster.local:6580",
      "endpoint": "api/v1/lineage",
      "tls": false
    },
    "team-b/feast-b": {
      "url": "https://feast-feast-b-lineage.team-b.svc.cluster.local:443",
      "endpoint": "api/v1/lineage",
      "tls": true
    }
  }
}
```

The `openlineage.yml` and `url` keys default to the most recently registered consumer.
In multi-consumer environments, producers should reference the `endpoints` key for explicit
consumer targeting.

#### Cross-Namespace Access

The discovery ConfigMap is created in the controller namespace. Producers in other
namespaces reference it by specifying the namespace:

```yaml
volumes:
  - name: ol-config
    configMap:
      name: feast-openlineage-config
      namespace: feast-operator-system  # controller namespace
```

> **Note:** Cross-namespace ConfigMap references in volume mounts require the Pod's
> ServiceAccount to have `get` access to the ConfigMap. The operator creates a Role
> and RoleBinding granting `system:authenticated` read access to the discovery ConfigMap.

### Feast UI Lineage Views

When the consumer is enabled, the lineage page in the Feast UI provides two views:

**OpenLineage Graph** (default when events exist)

- Shows lineage from all OpenLineage producers in a unified graph
- Nodes are color-coded by Feast object type (DataSource, Entity, FeatureView, FeatureService, etc.)
- Clicking a node opens a detail panel with description, schema, tags, features, entities, facets, and run history (for job nodes)
- Supports filtering by namespace

**Feast Only Lineage** (checkbox toggle)

- Shows the registry-based lineage view: DataSource → FeatureView → FeatureService, Entity → FeatureView, and OnDemandFeatureView relationships
- Powered entirely by the Feast registry — works independently of OpenLineage configuration
- When the consumer is enabled but has no events yet, this view is shown by default with the toggle visible to switch between views

### Cross-Producer Lineage Connectivity

The consumer automatically links datasets across different producers when they refer to the same physical data:

1. **Shared namespace + name** — when Airflow writes to `s3://bucket/path` and Spark reads from the same `s3://bucket/path`, the consumer automatically links them in the graph
2. **SymlinksDatasetFacet** — producers can declare aliases (e.g., Feast declaring its `driver_hourly_stats` is a symlink to `s3://bucket/features/driver_hourly_stats/`)
3. **dataSource URI matching** — datasets with matching `dataSource.uri` facets are linked even if their namespace or name differ

### RBAC for Lineage

The OpenLineage consumer integrates with Feast's existing RBAC — no new permissions or
`AuthzedAction` values are introduced:

- **Write access** (producers sending events): authenticated via API key in the `X-API-Key` header. Any authenticated producer can send events.
- **Read access** (UI viewing lineage): based on existing Feast project permissions. Users who can `DESCRIBE` a Feast project see lineage from that project's namespace **plus** any external namespaces mapped to it via `namespace_mapping`.

### Namespace Mapping

The `consumer.namespace_mapping` configuration is a read-side RBAC bridge that maps
external OpenLineage namespaces to Feast project names. It controls **who can see what**
in the lineage UI and API — it does **not** rewrite, reroute, or alter ingested events.

Events are always stored exactly as the producer sent them, with their original namespace
intact.

#### How It Works

When RBAC is enabled, each API query determines which namespaces the current user may
see:

1. List all Feast projects the user can `DESCRIBE` (existing Feast RBAC).
2. For each allowed project, resolve its OpenLineage namespace.
3. Scan `namespace_mapping` — for each entry whose **value** (Feast project name)
   matches an allowed project, add that entry's **key** (external namespace) to the
   allowed set.
4. Filter all query results to only include data from the allowed namespaces.

#### Configuration

In `feature_store.yaml`:

```yaml
openlineage:
  enabled: true
  namespace: feast
  consumer:
    enabled: true
    api_key: "change-me"                                                # pragma: allowlist secret
    namespace_mapping:
      "spark://ml-team": "ml_team"
      "airflow://prod-cluster": "ml_team"
      "ray://ml-team": "ml_team"
```

Or via environment variable (JSON format):

```bash
export FEAST_OPENLINEAGE_CONSUMER_NAMESPACE_MAPPING='{"spark://ml-team": "ml_team", "airflow://prod-cluster": "ml_team"}'
```

#### Cross-Producer Example

```
  Spark              Airflow                Ray               Feast
  namespace:         namespace:             namespace:         namespace:
  spark://ml-team    airflow://prod         ray://ml-team      ml_team

       │                  │                     │                 │
       └──────────────────┼─────────────────────┘                 │
                          ▼                                       │
                 POST /api/v1/lineage                              │
                          │                                       │
                          ▼                                       │
               ┌──────────────────────┐       (local wire)        │
               │  Feast OL Consumer   │ ◄─────────────────────────┘
               │  namespace_mapping:  │
               │    spark://ml-team   │
               │      → ml_team       │
               │    airflow://prod    │
               │      → ml_team       │
               │    ray://ml-team     │
               │      → ml_team       │
               └──────────────────────┘
                          │
                          ▼
                   Feast UI / API
                 (unified lineage view
                  filtered by RBAC)
```

A user with `DESCRIBE` permission on the `ml_team` Feast project can view lineage
from **all four producers** in a single unified graph.

#### Namespace Resolution for Feast Object Mapping

Beyond RBAC, `namespace_mapping` helps the event processor map incoming datasets
to Feast registry objects during ingest. When a dataset arrives with namespace
`spark://ml-team`, the processor resolves it to Feast project `ml_team` and can
match the dataset against known Feast objects in that project.

Resolution priority:

1. **Exact match** — `namespace_mapping["spark://ml-team"]` → `"ml_team"`
2. **Authority/path match** — for `scheme://authority/path` namespaces, try the
   authority+path portion
3. **Fallback** — use the last path segment as the project name

#### When Namespace Mapping Is Not Needed

- **Single-project setups**: if you only have one Feast project and no external
  producers, the default behavior (namespace = project name) works without mapping.
- **Feast-only lineage**: the Feast Only Lineage view operates purely on registry
  data and does not use `namespace_mapping`.
- **No RBAC**: when Feast RBAC is disabled, all namespaces are visible to all users.
  Mapping is still used for Feast object resolution during ingest.

### Per-Run Lineage (Run History)

The consumer tracks individual pipeline runs. When you click on a **job node** in the OpenLineage Graph, the detail panel shows a **Run History** section with:

- A table of past runs: run ID, status badge (COMPLETE, FAIL, RUNNING, ABORT), start time, and duration
- Click any run to see its specific **inputs and outputs** — the datasets that run consumed and produced

```bash
# List runs for a specific job
curl "http://localhost:8888/api/v1/lineage/openlineage/runs?job_namespace=spark://ml-team&job_name=feature_engineering"

# Get a single run with its I/O datasets
curl "http://localhost:8888/api/v1/lineage/openlineage/runs/{run_id}"
```

### Lineage Cleanup / Reset

#### Admin Reset Endpoint

Use `DELETE /api/v1/lineage/openlineage/reset` to purge lineage data:

```bash
# Purge ALL OpenLineage data
curl -X DELETE -H "X-API-Key: your-key" \
  http://localhost:8888/api/v1/lineage/openlineage/reset

# Purge only a specific namespace
curl -X DELETE -H "X-API-Key: your-key" \
  "http://localhost:8888/api/v1/lineage/openlineage/reset?namespace=airflow://prod-cluster"
```

#### Feast Teardown Hook

When you run `feast teardown`, Feast automatically cleans up OpenLineage data for the project's namespace (if the consumer is configured).

```bash
feast teardown
```

### Separate Lineage Server Deployment

For production environments requiring independent scaling of the lineage subsystem,
the OpenLineage consumer can be deployed as a **dedicated server**, separate from the
Feast registry and UI services.

#### When to Use Standalone vs Embedded

| Consideration | Embedded (default) | Standalone (`lineageServer`) |
|---------------|-------------------|------------------------------|
| **Deployment simplicity** | Single process, no extra resources | Separate Deployment + Service |
| **Lineage ingestion volume** | Low-to-moderate (< 100 events/min) | High-volume multi-producer environments |
| **Resource isolation** | Shares CPU/memory with registry | Independent scaling and resource limits |
| **Registry impact** | Heavy ingestion may affect registry latency | No impact on core Feast operations |
| **Retention pruning** | Runs in registry process | Runs exclusively in standalone server |
| **Recommended for** | Development, single-team setups | Production, multi-team or cross-platform lineage |

**Performance note:** In embedded mode, the lineage consumer shares database connections
and compute resources with the Feast registry. If multiple external producers (Spark,
Airflow, dbt) submit high-volume lineage events concurrently, this may increase registry
response times. For such environments, use the standalone deployment to isolate lineage
processing from core Feast operations.

#### Standalone CLI

Run the lineage consumer as a standalone process:

```bash
feast serve_lineage --host 0.0.0.0 --port 6580
```

This starts a dedicated server with only the OpenLineage consumer endpoints, independent
of the registry and UI services. It reads `feature_store.yaml` for the `openlineage.consumer`
configuration and optionally connects to the registry for RBAC enforcement when authorization
is configured.

#### Operator CRD

When deploying with the Feast Operator, add `lineageServer` to the consumer config
to create a separate Kubernetes Deployment:

```yaml
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: my-feast
spec:
  feastProject: my_project
  services:
    registry:
      local:
        persistence:
          store:
            type: sql
            secretRef:
              name: registry-db-secret
  openlineage:
    enabled: true
    consumer:
      enabled: true
      storeType: sql
      lineageServer:
        replicas: 2
        server:
          resources:
            requests:
              cpu: "250m"
              memory: "256Mi"
```

When `lineageServer` is configured:

1. **Separate Deployment**: The operator creates a `feast-<name>-lineage` Deployment
   running `feast serve_lineage`, with its own Service on port 6580.
2. **Auto-transport**: The producer `transport_url` on the main Feast Deployment is
   automatically configured to point to the lineage Service
   (`http://feast-<name>-lineage.<namespace>.svc.cluster.local:6580`).
3. **Full API on both**: Both the UI/registry server and the lineage server expose
   the complete consumer API (read and write). They share the same SQL database,
   so there is no conflict. The retention background task runs only on the
   standalone lineage server.
4. **Database**: The lineage server uses the consumer's `connectionStringSecretRef`
   if provided, otherwise falls back to the SQL registry database.
5. **RBAC**: When authz is configured in the CR, the lineage server connects to the
   registry Service as a remote client for permission checks.

#### Manual configuration

For non-operator deployments, set `standalone_server: true` in the consumer configuration
to delegate the retention background task to the standalone lineage server:

```yaml
# Main server feature_store.yaml
openlineage:
  enabled: true
  transport_type: http
  transport_url: http://lineage-server:6580
  transport_endpoint: api/v1/lineage
  consumer:
    enabled: true
    standalone_server: true  # delegates retention task to standalone server
```

```yaml
# Lineage server feature_store.yaml
project: my_project
openlineage:
  enabled: true
  consumer:
    enabled: true
    connection_string: postgresql://host/lineage_db
```

### API Contract

The Feast OpenLineage consumer exposes two categories of API endpoints:

1. **OpenLineage-standard ingest endpoints** — compliant with the [OpenLineage HTTP API spec (v2.0.2)](https://openlineage.io/apidocs/openapi/)
2. **Feast-specific query and admin endpoints** — for lineage visualization, retention management, and data administration

#### OpenLineage-Standard Endpoints (Ingest)

These endpoints are fully compatible with any OpenLineage producer (Spark, Airflow, dbt, Flink, etc.) using the standard HTTP transport.

| Method | Path | Description | Spec Reference |
|--------|------|-------------|----------------|
| `POST` | `/api/v1/lineage` | Receive a single `RunEvent`, `DatasetEvent`, or `JobEvent` | [OpenLineage spec `POST /lineage`](https://openlineage.io/apidocs/openapi/) |
| `POST` | `/api/v1/lineage/batch` | Receive an array of events in a single request | [OpenLineage spec `POST /lineage/batch`](https://openlineage.io/apidocs/openapi/) |

**Authentication:** API key via `X-API-Key` header or `Authorization: Bearer <key>` header.
- **Required** for: ingest endpoints (`POST /lineage`, `POST /lineage/batch`), admin endpoints (`POST .../retention/prune`, `DELETE .../reset`)
- **Not required** for: read-only query endpoints (graph, catalog, events, runs)
- When `consumer.api_key` is not configured, all endpoints are open.

Matches the OpenLineage Python client's `api_key` auth type.

**Request body:** `application/json` — accepts the standard OpenLineage event schema including all facets.

**Response codes:**

| Code | Meaning |
|------|---------|
| `201` | Single event accepted |
| `200` | Batch processed with summary (`{status, summary: {received, successful, failed}}`) |
| `204` | Batch processed silently (all events succeeded) |
| `207` | Batch partially succeeded (summary includes failure count) |
| `400` | Invalid JSON or unexpected body format |
| `401` | API key required but missing or invalid |

**Producer configuration example** (any OpenLineage producer):

```yaml
# OpenLineage Python client / Spark / Airflow config
transport:
  type: http
  url: http://feast-server:8888
  endpoint: api/v1/lineage
  auth:
    type: api_key
    apiKey: your-consumer-api-key
```

#### Feast-Specific Endpoints (Query & Admin)

These endpoints power the Feast lineage UI and are available for custom integrations.
They are not part of the OpenLineage spec — every consumer (Marquez, DataHub, Atlan)
defines its own query API.

**Lineage Graph:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/lineage/openlineage/graph` | Full lineage graph (nodes + edges + symlinks), with optional `?namespace=` and `?limit=`/`?offset=` |
| `GET` | `/api/v1/lineage/openlineage/graph/{node_type}/{namespace}/{name}` | Subgraph around a specific node, with `?depth=` and `?direction=` (upstream/downstream/both) |

**Catalog:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/lineage/openlineage/namespaces` | List all known namespaces |
| `GET` | `/api/v1/lineage/openlineage/jobs` | List all jobs across producers |
| `GET` | `/api/v1/lineage/openlineage/datasets` | List all datasets across producers |

**Events & Runs:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/lineage/openlineage/events` | List events with `?namespace=`, `?job_name=`, `?limit=`, `?offset=` |
| `GET` | `/api/v1/lineage/openlineage/runs` | List runs with `?job_namespace=`, `?job_name=`, `?limit=`, `?offset=` |
| `GET` | `/api/v1/lineage/openlineage/runs/{run_id}` | Single run detail with input/output datasets |

**Retention & Admin:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/lineage/openlineage/retention` | Current retention config and storage stats |
| `POST` | `/api/v1/lineage/openlineage/retention/prune` | Manually trigger retention pruning (requires API key) |
| `DELETE` | `/api/v1/lineage/openlineage/reset` | Purge all or namespace-specific lineage data (requires API key) |

**RBAC filtering:** All query endpoints automatically filter results by the calling user's
permitted namespaces when Feast authz is configured. The `namespace_mapping` config maps
external producer namespaces to Feast projects for access control.

#### API Availability in Deployment Modes

| Deployment Mode | Ingest Endpoints | Query Endpoints | Retention Task |
|-----------------|-----------------|-----------------|----------------|
| **Embedded** (consumer in UI/registry server) | Available | Available | Runs here |
| **Standalone** (separate lineage server) | Available on both servers | Available on both servers | Runs on lineage server only |

Both servers share the same SQL database, so all endpoints are fully functional on either server.
The standalone lineage server exclusively owns the background retention pruning task to avoid duplicate work.

### Database Schema

The consumer creates the following tables automatically during initial startup:

| Table | Purpose |
|-------|---------|
| `openlineage_events` | Raw event storage with JSON payloads |
| `openlineage_jobs` | Deduplicated job records with producer, description, and facets |
| `openlineage_datasets` | Deduplicated dataset records with schema, facets, and Feast object mapping |
| `openlineage_runs` | Run lifecycle tracking (START/COMPLETE/FAIL) |
| `openlineage_run_io` | Input/output relationships between runs and datasets |
| `openlineage_lineage_edges` | Materialized lineage graph edges for efficient traversal |
| `openlineage_dataset_symlinks` | Cross-producer dataset linking via `SymlinksDatasetFacet` and `dataSource` URI matching |

By default these tables are created in the **same database** as the SQL registry. Set `consumer.connection_string` to use a separate database.
