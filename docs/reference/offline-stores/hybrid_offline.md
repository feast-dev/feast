# Hybrid Offline Store

## Overview

The Hybrid Offline Store is a specialized store that routes operations to different underlying offline stores based on the data source type. This enables you to use multiple types of data sources (e.g., BigQuery, Redshift, Snowflake, File) within the same Feast deployment.

## When to Use

Consider using the Hybrid Offline Store when:

- You have data spread across multiple data platforms
- You want to gradually migrate from one data source to another
- Different teams in your organization use different data storage technologies
- You need to optimize cost by using specialized stores for specific workloads

## Configuration

### Setting Up the Hybrid Offline Store

To use the Hybrid Offline Store, you need to configure it in your `feature_store.yaml` file:

```yaml
project: my_project
registry: registry.db
provider: local
offline_store:
  type: feast.infra.offline_stores.hybrid_offline_store.HybridOfflineStore
  offline_stores:
    - type: bigquery
      dataset: feast_dataset
      project_id: gcp_project_id
    - type: redshift
      cluster_id: my_redshift_cluster
      region: us-west-2
      user: admin
      database: feast
      s3_staging_location: s3://feast-bucket/staging
    - type: file
      path: /data/feast
```

### Supported Offline Stores

The Hybrid Offline Store supports all of Feast's offline stores:

- BigQuery
- Redshift
- Snowflake
- File (Parquet, CSV)
- Postgres
- Spark
- Trino
- Custom offline stores

## Usage

### Defining Feature Views with Different Source Types

When using the Hybrid Offline Store, you can define feature views with different source types:

```python
# BigQuery source
bq_source = BigQuerySource(
    table="my_table",
    event_timestamp_column="timestamp",
    created_timestamp_column="created_ts",
)

# File source
file_source = FileSource(
    path="/data/transactions.parquet",
    event_timestamp_column="timestamp",
    created_timestamp_column="created_ts",
)

# Define feature views with different sources
driver_stats_bq = FeatureView(
    name="driver_stats_bq",
    entities=[driver],
    ttl=timedelta(days=1),
    source=bq_source,
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int32),
    ],
)

driver_stats_file = FeatureView(
    name="driver_stats_file",
    entities=[driver],
    ttl=timedelta(days=1),
    source=file_source,
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int32),
    ],
)
```

### How Routing Works

The Hybrid Offline Store routes operations to the appropriate underlying store based on the source type:

1. The source type is determined by examining the class name of the data source (e.g., `BigQuerySource`, `FileSource`).
2. The source store type is extracted from the class name (e.g., `bigquery`, `file`).
3. The operation is delegated to the matching offline store configuration.

## Limitations

- All feature views used in a single `get_historical_features` call must have the same source type.
- Custom data sources must follow the naming convention `{SourceType}Source` (e.g., `BigQuerySource`, `FileSource`).
- Each offline store configuration must have a unique `type` value.

## Implementation Details

The Hybrid Offline Store acts as a router, delegating operations to the appropriate underlying store. Key operations that are routed include:

- `get_historical_features`: Retrieves historical feature values for training or batch scoring
- `pull_latest_from_table_or_query`: Pulls the latest feature values from a table or query
- `pull_all_from_table_or_query`: Pulls all feature values from a table or query for a specified time range
- `offline_write_batch`: Writes a batch of feature values to the offline store

## Troubleshooting

### Common Issues

#### Feature Views with Different Source Types

If you encounter an error like:
```
ValueError: All feature views must have the same source type
```

This means you're trying to retrieve historical features for feature views with different source types in a single call. Split the call into multiple calls, one per source type.

#### Offline Store Configuration Not Found

If you encounter an error like:
```
ValueError: No offline store configuration found for source type 'X'
```

Make sure you've included an offline store configuration for each source type you're using.

## Examples

### Complete Feature Repository Example

```python
from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource, BigQuerySource, FeatureStore
from feast.types import Float32, Int32, Int64

# Define an entity
driver = Entity(
    name="driver_id",
    value_type=Int64,
    description="Driver ID",
)

# BigQuery source
bq_source = BigQuerySource(
    table="my_project.my_dataset.driver_stats",
    event_timestamp_column="event_timestamp",
)

# File source
file_source = FileSource(
    path="/data/driver_activity.parquet",
    event_timestamp_column="event_timestamp",
)

# Feature view with BigQuery source
driver_stats_bq = FeatureView(
    name="driver_stats_bq",
    entities=[driver],
    ttl=timedelta(days=1),
    source=bq_source,
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int32),
    ],
)

# Feature view with File source
driver_activity_file = FeatureView(
    name="driver_activity_file",
    entities=[driver],
    ttl=timedelta(days=1),
    source=file_source,
    schema=[
        Field(name="active_hours", dtype=Float32),
        Field(name="driving_days", dtype=Int32),
    ],
)

# Apply feature views
fs = FeatureStore(repo_path="../../how-to-guides/customizing-feast")
fs.apply([driver, driver_stats_bq, driver_activity_file])

# Get historical features from BigQuery source
training_df_bq = fs.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_stats_bq:conv_rate",
        "driver_stats_bq:acc_rate",
        "driver_stats_bq:avg_daily_trips",
    ],
).to_df()

# Get historical features from File source
training_df_file = fs.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_activity_file:active_hours",
        "driver_activity_file:driving_days",
    ],
).to_df()
```

## Conclusion

The Hybrid Offline Store provides flexibility in working with multiple data sources while maintaining the simplicity of the Feast API. By routing operations to the appropriate underlying store, it allows you to leverage the strengths of different data platforms within a single Feast deployment.
