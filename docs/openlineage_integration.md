# OpenLineage Integration for Feast

This document describes the OpenLineage integration added to Feast for standardized lineage event emission.

## Overview

OpenLineage is an open framework for capturing lineage metadata about jobs, runs, and datasets. This integration enables Feast to emit standardized lineage events during feature materialization and retrieval operations.

## Configuration

### Basic Configuration

Add the `openlineage` section to your `feature_store.yaml`:

```yaml
project: my_project
registry: data/registry.db
provider: local

openlineage:
  enabled: true
  transport_type: http
  transport_config:
    url: http://localhost:5000
  namespace: feast
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `false` | Enable/disable OpenLineage event emission |
| `transport_type` | str | `"http"` | Transport type: `http`, `kafka`, `console`, or `file` |
| `transport_config` | dict | `{}` | Transport-specific configuration |
| `namespace` | str | `"feast"` | OpenLineage namespace for all events |
| `emit_materialization_events` | bool | `true` | Emit events for materialization runs |
| `emit_retrieval_events` | bool | `false` | Emit events for feature retrieval |

### Transport Configuration Examples

#### HTTP Transport (Marquez)
```yaml
openlineage:
  enabled: true
  transport_type: http
  transport_config:
    url: http://marquez-api:5000
    timeout: 5.0
    verify: true
```

#### Kafka Transport
```yaml
openlineage:
  enabled: true
  transport_type: kafka
  transport_config:
    bootstrap.servers: localhost:9092
    topic: openlineage.events
```

#### File Transport
```yaml
openlineage:
  enabled: true
  transport_type: file
  transport_config:
    log_file_path: /tmp/openlineage_events.json
```

#### Console Transport (Development)
```yaml
openlineage:
  enabled: true
  transport_type: console
  transport_config: {}
```

## Usage

### Materialization with OpenLineage

When OpenLineage is enabled, materialization operations automatically emit lineage events:

```python
from feast import FeatureStore
from datetime import datetime, timedelta

# Initialize feature store with OpenLineage configured
fs = FeatureStore(repo_path=".")

# Materialize features - lineage events are emitted automatically
fs.materialize(
    start_date=datetime.utcnow() - timedelta(days=1),
    end_date=datetime.utcnow()
)
```

### Incremental Materialization

Incremental materialization also emits lineage events:

```python
fs.materialize_incremental(
    end_date=datetime.utcnow()
)
```

## Emitted Events

### Event Types

The integration emits three types of events:

1. **START**: Emitted at the beginning of a materialization run
2. **COMPLETE**: Emitted when materialization completes successfully
3. **FAIL**: Emitted when materialization fails

### Event Structure

Each event includes:

- **Job**: Identifies the Feast materialization job
  - Name: `feast_{project}_materialize_{feature_view_name}`
  - Namespace: Configured namespace (default: `feast`)
  
- **Run**: Contains run metadata
  - Run ID: Unique identifier for the materialization run
  - Nominal time: Start and end timestamps for the materialization window
  - Custom facets: Feast-specific metadata (feature view details, features, entities)

- **Datasets**:
  - **Inputs**: Offline store datasets (batch sources)
  - **Outputs**: Online store datasets

### Custom Facets

Feast adds custom facets to enrich lineage metadata:

- **feast facet**: Contains Feast-specific information
  - `featureViewName`: Name of the feature view
  - `features`: List of feature names
  - `entities`: List of entity names
  - `batchSourceType`: Type of batch data source

## Integration with Lineage Systems

### Marquez

Marquez is an open-source metadata service for the collection, aggregation, and visualization of dataset and job metadata.

To integrate with Marquez:

1. Run Marquez (via Docker):
```bash
docker run -p 5000:5000 marquezproject/marquez:latest
```

2. Configure Feast:
```yaml
openlineage:
  enabled: true
  transport_type: http
  transport_config:
    url: http://localhost:5000
```

3. View lineage in the Marquez UI at `http://localhost:3000`

### Other OpenLineage-compatible Systems

The integration works with any OpenLineage-compatible system:

- Astronomer (Apache Airflow with OpenLineage)
- DataHub with OpenLineage support
- Egeria with OpenLineage integration
- Custom OpenLineage consumers

## Architecture

### Components

1. **OpenLineageConfig**: Pydantic model for configuration
2. **OpenLineageClient**: Wrapper around the OpenLineage Python SDK
3. **Feature Store Integration**: Automatic event emission in materialize methods

### Event Flow

```
┌─────────────────┐
│ materialize()   │
└────────┬────────┘
         │
         ├─> START event
         │   - Job metadata
         │   - Input datasets
         │   - Output datasets
         │
         ├─> Execute materialization
         │
         └─> COMPLETE/FAIL event
             - Run duration
             - Success/failure status
```

## Best Practices

1. **Start with Console Transport**: Use `console` transport during development
2. **Use Namespaces**: Set unique namespaces for different environments (dev, staging, prod)
3. **Monitor Performance**: OpenLineage events are emitted synchronously; consider the impact on materialization time
4. **Selective Emission**: Disable retrieval events in production unless needed (they can be high-volume)

## Troubleshooting

### Events Not Appearing

1. Check if OpenLineage is enabled: `enabled: true`
2. Verify transport configuration (URL, credentials)
3. Check logs for errors: Feast logs OpenLineage errors at ERROR level
4. Test connectivity to the lineage backend

### Import Errors

If you see `ModuleNotFoundError: No module named 'openlineage'`:

```bash
pip install openlineage-python
```

Or add it to your requirements:
```
feast[openlineage]>=0.39.0
```

### Performance Impact

If materialization is slow after enabling OpenLineage:

1. Check network latency to the lineage backend
2. Consider using async transport (Kafka)
3. Disable retrieval events: `emit_retrieval_events: false`

## Examples

See `examples/feature_store_openlineage.yaml` for a complete configuration example.

## References

- [OpenLineage Specification](https://openlineage.io/docs/spec/)
- [OpenLineage Python Client](https://openlineage.io/docs/client/python)
- [Marquez](https://marquezproject.ai/)
