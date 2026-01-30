# Feast OpenLineage Integration Example

This example demonstrates Feast's **native OpenLineage integration** for automatic data lineage tracking.

For full documentation, see the [OpenLineage Reference](../../docs/reference/openlineage.md).

## Prerequisites

```bash
pip install feast[openlineage]
```

## Running the Demo

1. Start Marquez:
```bash
docker run -p 5000:5000 -p 3000:3000 marquezproject/marquez
```

2. Run the demo:
```bash
python openlineage_demo.py --url http://localhost:5000
```

3. View lineage at http://localhost:3000

## What the Demo Shows

The demo creates a sample feature repository and demonstrates:

- **Entity**: `driver_id`
- **DataSource**: `driver_stats_source` (Parquet file)
- **FeatureView**: `driver_hourly_stats` with features like conversion rate, acceptance rate
- **FeatureService**: `driver_stats_service` aggregating features

When you run the demo, it will:
1. Create the feature store with OpenLineage enabled
2. Apply the features (emits lineage events)
3. Materialize features (emits START/COMPLETE events)
4. Retrieve features (demonstrates online feature retrieval)

## Lineage Graph

After running the demo, you'll see this lineage in Marquez:

```
driver_stats_source ──┐
                      ├──→ feast_feature_views_openlineage_demo ──→ driver_hourly_stats
driver_id ────────────┘                                                    │
                                                                           ▼
                                            feature_service_driver_stats_service ──→ driver_stats_service
```

## Learn More

- [Feast OpenLineage Reference](../../docs/reference/openlineage.md)
- [OpenLineage Documentation](https://openlineage.io/docs)
- [Marquez Project](https://marquezproject.ai)
