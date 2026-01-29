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
| `transport_type` | `http` | Transport type: `http`, `file`, `kafka` |
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
