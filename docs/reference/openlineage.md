# OpenLineage Integration

This module provides **native integration** between Feast and [OpenLineage](https://openlineage.io/), enabling automatic data lineage tracking for ML feature engineering workflows.

## Overview

When enabled, the integration **automatically** emits OpenLineage events for:

- **Registry changes** - Events when feature views, feature services, and entities are applied
- **Feature materialization** - START, COMPLETE, and FAIL events when features are materialized

**No code changes required** - just enable OpenLineage in your `feature_store.yaml`!

## Installation

OpenLineage is an optional dependency. Install it with:

```bash
pip install openlineage-python
```

Or install Feast with the OpenLineage extra:

```bash
pip install feast[openlineage]
```

## Configuration

Add the `openlineage` section to your `feature_store.yaml`:

```yaml
project: my_project
registry: data/registry.db
provider: local
online_store:
  type: sqlite
  path: data/online_store.db

openlineage:
  enabled: true
  transport_type: http
  transport_url: http://localhost:5000
  transport_endpoint: api/v1/lineage
  namespace: feast
  emit_on_apply: true
  emit_on_materialize: true
```

Once configured, all Feast operations will automatically emit lineage events.

### Environment Variables

You can also configure via environment variables:

```bash
export FEAST_OPENLINEAGE_ENABLED=true
export FEAST_OPENLINEAGE_TRANSPORT_TYPE=http
export FEAST_OPENLINEAGE_URL=http://localhost:5000
export FEAST_OPENLINEAGE_ENDPOINT=api/v1/lineage
export FEAST_OPENLINEAGE_NAMESPACE=feast
```

## Usage

Once configured, lineage is tracked automatically:

```python
from feast import FeatureStore
from datetime import datetime, timedelta

# Create FeatureStore - OpenLineage is initialized automatically if configured
fs = FeatureStore(repo_path="feature_repo")

# Apply operations emit lineage events automatically
fs.apply([driver_entity, driver_hourly_stats_view])

# Materialize emits START, COMPLETE/FAIL events automatically
fs.materialize(
    start_date=datetime.now() - timedelta(days=1),
    end_date=datetime.now()
)

```

## Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `enabled` | `false` | Enable/disable OpenLineage integration |
| `transport_type` | `None` | Transport type: `http`, `console`, `file`, `kafka`. When unset, defers to OpenLineage SDK defaults. |
| `transport_url` | - | URL for HTTP transport (required) |
| `transport_endpoint` | `api/v1/lineage` | API endpoint for HTTP transport |
| `api_key` | - | Optional API key for authentication |
| `namespace` | `feast` | Namespace for lineage events (uses project name if set to "feast") |
| `producer` | `feast` | Producer identifier |
| `emit_on_apply` | `true` | Emit events on `feast apply` |
| `emit_on_materialize` | `true` | Emit events on materialization |

## Lineage Graph Structure

When you run `feast apply`, Feast creates a lineage graph that matches the Feast UI:

```
DataSources ──┐
              ├──→ feast_feature_views_{project} ──→ FeatureViews
Entities ─────┘                                          │
                                                         │
                                                         ▼
                                     feature_service_{name} ──→ FeatureService
```

**Jobs created:**
- `feast_feature_views_{project}`: Shows DataSources + Entities → FeatureViews
- `feature_service_{name}`: Shows specific FeatureViews → FeatureService (one per service)

**Datasets include:**
- Schema with feature names, types, descriptions, and tags
- Feast-specific facets with metadata (TTL, entities, owner, etc.)
- Documentation facets with descriptions

## Transport Types

### HTTP Transport (Recommended for Production)

```yaml
openlineage:
  enabled: true
  transport_type: http
  transport_url: http://marquez:5000
  transport_endpoint: api/v1/lineage
  api_key: your-api-key  # Optional
```

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

## Custom Feast Facets

The integration includes custom Feast-specific facets in lineage events:

### FeastFeatureViewFacet

Captures metadata about feature views:
- `name`: Feature view name
- `ttl_seconds`: Time-to-live in seconds
- `entities`: List of entity names
- `features`: List of feature names
- `online_enabled` / `offline_enabled`: Store configuration
- `description`: Feature view description
- `tags`: Key-value tags

### FeastFeatureServiceFacet

Captures metadata about feature services:
- `name`: Feature service name
- `feature_views`: List of feature view names
- `feature_count`: Total number of features
- `description`: Feature service description
- `tags`: Key-value tags

### FeastMaterializationFacet

Captures materialization run metadata:
- `feature_views`: Feature views being materialized
- `start_date` / `end_date`: Materialization window
- `rows_written`: Number of rows written

## Lineage Visualization

### Option 1: Feast UI (Built-in)

Feast includes a built-in OpenLineage consumer that can receive, store, and visualize lineage from **all** OpenLineage producers (Airflow, Spark, dbt, Feast itself, etc.) directly in the Feast UI. See the [OpenLineage Consumer](#openlineage-consumer) section below.

### Option 2: Marquez

Use [Marquez](https://marquezproject.ai/) to visualize your Feast lineage:

```bash
# Start Marquez
docker run -p 5000:5000 -p 3000:3000 marquezproject/marquez

# Configure Feast to emit to Marquez (in feature_store.yaml)
# openlineage:
#   enabled: true
#   transport_type: http
#   transport_url: http://localhost:5000
```

Then access the Marquez UI at http://localhost:3000 to see your feature lineage.

## Namespace Behavior

- If `namespace` is set to `"feast"` (default): Uses project name as namespace (e.g., `my_project`)
- If `namespace` is set to a custom value: Uses `{namespace}/{project}` (e.g., `custom/my_project`)

## Feast to OpenLineage Mapping

| Feast Concept | OpenLineage Concept |
|---------------|---------------------|
| DataSource | InputDataset |
| FeatureView | OutputDataset (of feature views job) / InputDataset (of feature service job) |
| Feature | Schema field |
| Entity | InputDataset |
| FeatureService | OutputDataset |
| Materialization | RunEvent (START/COMPLETE/FAIL) |

---

## OpenLineage Consumer

Feast can act as an **OpenLineage consumer**, receiving lineage events from any OpenLineage-compatible producer and displaying them in the Feast UI. This eliminates the need for a separate Marquez deployment when you want to visualize cross-system data lineage alongside your feature store.

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

When the consumer is **not** enabled, the Feast UI shows only the original registry-based lineage view — no tabs are added.

### Enabling the Consumer

Add the `consumer` section under `openlineage` in your `feature_store.yaml`:

```yaml
project: my_project
registry:
  registry_type: sql
  path: postgresql://user:****@host:5432/feast  # pragma: allowlist secret

openlineage:
  enabled: true
  namespace: my_project
  consumer:
    enabled: true
    store_type: sql
    # Optional: separate database for lineage storage.
    # If omitted, the SQL registry database is reused.
    # connection_string: postgresql://user:****@host:5432/feast_lineage
    api_key: "change-me"  # pragma: allowlist secret
    namespace_mapping:
      airflow_ns: my_project
      spark_ns: my_project
```

Or via environment variables:

```bash
export FEAST_OPENLINEAGE_CONSUMER_ENABLED=true
export FEAST_OPENLINEAGE_CONSUMER_STORE_TYPE=sql
export FEAST_OPENLINEAGE_CONSUMER_API_KEY=change-me  # pragma: allowlist secret
# Optional separate DB:
# export FEAST_OPENLINEAGE_CONSUMER_CONNECTION_STRING=postgresql://...
```

### Consumer Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `consumer.enabled` | `false` | Enable the OpenLineage consumer |
| `consumer.store_type` | `sql` | Storage backend type. Currently only `sql` is supported |
| `consumer.connection_string` | - | Optional separate database connection string. If omitted, reuses the SQL registry database |
| `consumer.api_key` | - | API key that producers must provide when sending events |
| `consumer.namespace_mapping` | `{}` | Maps OpenLineage namespaces to Feast projects for RBAC scoping |

### Consumer API Endpoints

When the consumer is enabled, the following endpoints are available on the Feast REST registry server:

#### Event Receiver (Producer-facing)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/lineage` | `POST` | Receive a single OpenLineage event (or array of events) |
| `/api/v1/lineage/batch` | `POST` | Receive a batch of OpenLineage events |

Both endpoints require the `X-API-Key` header (or `Authorization: Bearer <key>`) if `consumer.api_key` is configured.

#### Admin Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/lineage/openlineage/reset` | `DELETE` | Purge all OpenLineage data. Accepts optional `?namespace=X` to delete only a specific namespace. Requires API key. |

#### OpenLineage Query Endpoints (UI-facing)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/lineage/openlineage/graph` | `GET` | Full lineage graph with all nodes, edges, and symlinks |
| `/lineage/openlineage/graph/{node_type}/{namespace}/{name}` | `GET` | Lineage graph centered on a specific node |
| `/lineage/openlineage/events` | `GET` | Browse stored events with filtering |
| `/lineage/openlineage/jobs` | `GET` | List all known OpenLineage jobs |
| `/lineage/openlineage/datasets` | `GET` | List all known OpenLineage datasets |
| `/lineage/openlineage/runs` | `GET` | List runs with optional `?job_namespace=X&job_name=Y` filtering |
| `/lineage/openlineage/runs/{run_id}` | `GET` | Single run detail with input/output datasets |

#### Registry Query Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/lineage/registry` | `GET` | Feast registry lineage (entities, feature views, services) |
| `/lineage/registry/all` | `GET` | All registry objects with full metadata |
| `/lineage/objects/{object_type}/{object_name}` | `GET` | Detail for a specific registry object |
| `/lineage/complete` | `GET` | Complete registry lineage with relationships |
| `/lineage/complete/all` | `GET` | Complete registry lineage for all objects |

### Configuring Producers to Send Events to Feast

Configure any OpenLineage producer to send events to your Feast instance:

#### Airflow

```python
# In airflow.cfg or environment
OPENLINEAGE_URL = "http://feast-registry:8080/api"
OPENLINEAGE_API_KEY = "change-me"  # pragma: allowlist secret
```

#### Spark

```properties
spark.openlineage.transport.type=http
spark.openlineage.transport.url=http://feast-registry:8080/api
spark.openlineage.transport.endpoint=/v1/lineage
spark.openlineage.transport.auth.type=api_key
spark.openlineage.transport.auth.apiKey=change-me
```

#### dbt

```yaml
# In profiles.yml or environment
OPENLINEAGE_URL: "http://feast-registry:8080/api"
OPENLINEAGE_API_KEY: "change-me"  # pragma: allowlist secret
```

#### Feast (Self-reporting)

When both the OpenLineage producer and consumer are enabled, Feast's own events (from `feast apply`, materialization, etc.) are automatically ingested into the local consumer store — no HTTP transport is needed.

```yaml
# In feature_store.yaml
openlineage:
  enabled: true
  namespace: my_project
  consumer:
    enabled: true
    api_key: change-me  # pragma: allowlist secret
```

### Feast UI Lineage Views

When the consumer is enabled, the lineage page in the Feast UI shows two tabs:

**Lineage tab**

- **OpenLineage Graph** (default) — shows lineage from all OpenLineage producers with cross-producer connectivity. Nodes are color-coded by producer (colors generated dynamically). The graph supports filtering by type, producer, and object name. Clicking a node opens a **detail panel** showing description, schema, tags, features, entities, data quality metrics, data source info, other facets, and **run history** (for job nodes — see [Per-Run Lineage](#per-run-lineage-run-history)).
- **Feast Only Lineage** (checkbox) — switches to the original Feast registry view (DataSource → FeatureView → FeatureService) powered entirely by the Feast registry.

**Events tab**

- Browse individual OpenLineage events with filtering by event type, job name, and run ID. Expand any event to inspect the full JSON payload.

### Cross-Producer Lineage Connectivity

The consumer automatically links datasets across different producers when they refer to the same physical data. Linking mechanisms:

1. **Shared namespace + name** — If Airflow writes to `s3://bucket/path` and Spark reads from the same `s3://bucket/path`, the graph connects them automatically.
2. **SymlinksDatasetFacet** — Producers can declare aliases. For example, Feast can declare that its internal `driver_hourly_stats` is a symlink to the Spark output at `s3://bucket/features/driver_hourly_stats/`.
3. **dataSource URI matching** — Datasets with matching `dataSource.uri` facets are linked even if their namespace or name differ.

Compatible producers include Airflow, Spark, dbt, Flink, Feast, Dagster, and Great Expectations.

### RBAC for Lineage

The OpenLineage consumer integrates with Feast's existing RBAC:

- **Write access** (producers sending events): Authenticated via API key in the `X-API-Key` header
- **Read access** (UI viewing lineage): Namespace-based filtering maps OpenLineage namespaces to Feast projects. Users see only lineage data for namespaces they have access to via the `namespace_mapping` configuration

### Lineage Cleanup / Reset

Over time the OpenLineage store accumulates historical data. Two mechanisms are provided for cleanup:

#### Admin Reset Endpoint

Use the `DELETE /lineage/openlineage/reset` endpoint to purge lineage data. The endpoint requires the same API key used for event ingestion.

```bash
# Purge ALL OpenLineage data
curl -X DELETE -H "X-API-Key: your-key" \
  http://localhost:8080/api/v1/lineage/openlineage/reset

# Purge only a specific namespace
curl -X DELETE -H "X-API-Key: your-key" \
  "http://localhost:8080/api/v1/lineage/openlineage/reset?namespace=airflow://prod-cluster"
```

A full purge deletes data from all seven `openlineage_*` tables. A namespace-scoped purge deletes jobs, datasets, runs, events, edges, and symlinks associated with that namespace, leaving other namespaces intact.

#### Feast Teardown Hook

When you run `feast teardown`, Feast automatically cleans up OpenLineage data for the project's namespace (if the consumer is configured). This ensures that tearing down a Feast project doesn't leave orphaned lineage data behind.

```bash
# Tears down the Feast project AND its OpenLineage lineage
feast teardown
```

### Per-Run Lineage (Run History)

The consumer tracks individual pipeline runs in the `openlineage_runs` table. When you click on a **job node** in the OpenLineage Graph, the detail panel shows a **Run History** section with:

- A table of past runs: truncated run ID, status badge (COMPLETE, FAIL, RUNNING, ABORT), start time, and duration
- Click any run to expand its **inputs and outputs** — the specific datasets that run consumed and produced

#### Run History API

```bash
# List runs for a specific job
curl "http://localhost:8080/api/v1/lineage/openlineage/runs?job_namespace=spark://emr-cluster&job_name=feature_engineering"

# Get a single run with its I/O datasets
curl "http://localhost:8080/api/v1/lineage/openlineage/runs/{run_id}"
```

The run detail response includes `inputs` and `outputs` arrays, each containing the dataset namespace, name, and any I/O facets recorded by the producer.

### Database Schema

The consumer creates the following tables (automatically on first startup):

| Table | Purpose |
|-------|---------|
| `openlineage_events` | Raw event storage with JSON payloads |
| `openlineage_jobs` | Deduplicated job records with producer, description, and facets |
| `openlineage_datasets` | Deduplicated dataset records with schema, facets, and Feast mapping |
| `openlineage_runs` | Run lifecycle tracking (START/COMPLETE/FAIL) |
| `openlineage_run_io` | Input/output relationships between runs and datasets |
| `openlineage_lineage_edges` | Materialized lineage graph edges for efficient traversal |
| `openlineage_dataset_symlinks` | Cross-producer dataset linking via `SymlinksDatasetFacet` and `dataSource` URI matching |

By default these tables are created in the **same database** as the SQL registry (hybrid storage). Set `consumer.connection_string` to store them in a separate database instead.
