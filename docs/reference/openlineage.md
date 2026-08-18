# OpenLineage Integration

Feast provides **native integration** with [OpenLineage](https://openlineage.io/), enabling automatic data lineage tracking for ML feature engineering workflows. Feast can act as both a **producer** (emitting lineage events) and a **consumer** (receiving and displaying lineage from any OpenLineage-compatible system).

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
```

Open http://localhost:8888 and navigate to the **Lineage** tab. You will see the full lineage graph — both the Feast registry view and the OpenLineage view.

## Overview

When enabled, the integration **automatically** emits OpenLineage events for:

- **Registry changes** — events when feature views, on-demand feature views, feature services, entities, data sources, and saved datasets are applied
- **Feature materialization** — START, COMPLETE, and FAIL events when features are materialized

**No code changes required** — just enable OpenLineage in your `feature_store.yaml`.

## Prerequisites

- **SQL registry required for consumer**: The OpenLineage consumer stores lineage data in SQL tables. If you enable the consumer, your Feast registry must use `registry_type: sql` (SQLite, PostgreSQL, MySQL). File-based registries are not supported for the consumer. The producer works with any registry type.

## Producer Configuration

Add the `openlineage` section to your `feature_store.yaml`:

```yaml
openlineage:
  enabled: true
  transport_type: http
  transport_url: http://localhost:5000
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
export FEAST_OPENLINEAGE_URL=http://localhost:5000
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
  transport_url: http://marquez:5000
  transport_endpoint: api/v1/lineage
  api_key: your-api-key
```

### Console Transport (Development)

```yaml
openlineage:
  enabled: true
  transport_type: console
```

Events are printed to stdout — useful for debugging.

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

Feast can act as an **OpenLineage consumer**, receiving lineage events from any OpenLineage-compatible producer and displaying them in the Feast UI. This eliminates the need for a separate Marquez deployment.

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
| `consumer.connection_string` | — | Optional separate database connection string. If omitted, reuses the SQL registry database |
| `consumer.api_key` | — | API key that producers must provide when sending events |
| `consumer.namespace_mapping` | `{}` | Maps external OpenLineage namespaces to Feast project names for RBAC scoping (see [Namespace Mapping](#namespace-mapping)) |
| `consumer.retention_days` | `30` | Number of days to retain events and runs. Set to `0` to disable pruning |
| `consumer.retention_check_interval_hours` | `6` | How often the background pruning task runs (hours) |

### Event Retention

The consumer automatically prunes old events and runs to prevent unbounded storage growth. By default, data older than **30 days** is deleted every **6 hours**.

**What gets pruned:** Events (`openlineage_events`) and runs (`openlineage_runs`, `openlineage_run_io`).

**What is preserved:** The current-state graph (jobs, datasets, edges, symlinks) is never pruned. These tables represent the latest lineage topology, not historical data.

```yaml
openlineage:
  consumer:
    enabled: true
    retention_days: 7          # Keep only 7 days of events
    retention_check_interval_hours: 1   # Check every hour
```

To disable automatic pruning entirely:

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

The `feast ui` command starts a single server that handles everything:

- Serves the React UI with lineage visualization
- Exposes the OpenLineage consumer endpoints (both ingestion and query)
- Reads from the Feast registry

```bash
feast ui --port 8888
```

When both producer and consumer are enabled, Feast's own events (from `feast apply`, materialization) are **automatically ingested** into the local consumer store via an in-process wiring — no HTTP transport configuration is needed for self-reporting.

```yaml
# Minimal config for producer + consumer (self-contained)
openlineage:
  enabled: true
  transport_type: console   # still prints to stdout for debugging
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

Configure any OpenLineage producer to send events to Feast. The ingestion endpoint is `POST /api/v1/lineage`.

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

When both the OpenLineage producer and consumer are enabled in the same `feature_store.yaml`, Feast's own events (from `feast apply`, materialization) are automatically ingested into the local consumer store via an in-process wiring — no HTTP transport is needed.

```yaml
openlineage:
  enabled: true
  namespace: my_project
  consumer:
    enabled: true
    api_key: change-me                                                  # pragma: allowlist secret
```

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

1. **Shared namespace + name** — if Airflow writes to `s3://bucket/path` and Spark reads from the same `s3://bucket/path`, the graph connects them
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

A user who can `DESCRIBE` the `ml_team` Feast project sees lineage from **all four
producers** in a single unified graph.

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

### Database Schema

The consumer creates the following tables automatically on first startup:

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
