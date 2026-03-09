---
name: feast-feature-engineering
description: Use this skill when building feature stores with Feast. Covers feature definitions, materialization, online/offline retrieval, and on-demand transformations.
---

# Feast Feature Engineering

## Quick Start

```bash
pip install feast
feast init my_project
cd my_project/feature_repo
feast apply
feast ui
```

## Defining Features

### feature_store.yaml
```yaml
project: my_project
registry: data/registry.db
provider: local
online_store:
  type: sqlite
  path: data/online_store.db
offline_store:
  type: file
entity_key_serialization_version: 3
```

### Feature Definitions (Python)

```python
from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int64, String

driver = Entity(
    name="driver",
    join_keys=["driver_id"],
)

driver_stats_source = FileSource(
    name="driver_hourly_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
)

driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    online=True,
    source=driver_stats_source,
    ttl=timedelta(hours=2),
)
```

### On-Demand Feature Views (transformations)
```python
from feast import on_demand_feature_view, Field
from feast.types import Float64
import pandas as pd

@on_demand_feature_view(
    sources=[driver_stats_fv],
    schema=[Field(name="conv_rate_plus_acc", dtype=Float64)],
)
def transformed_conv_rate(inputs: pd.DataFrame) -> pd.DataFrame:
    df = pd.DataFrame()
    df["conv_rate_plus_acc"] = inputs["conv_rate"] + inputs["acc_rate"]
    return df
```

## Materialization

```bash
# Materialize features up to now
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")

# Materialize a specific time range
feast materialize 2023-01-01T00:00:00 2023-12-31T23:59:59
```

## Feature Retrieval

### Historical (training data)
```python
from feast import FeatureStore
import pandas as pd
from datetime import datetime

store = FeatureStore(repo_path=".")

entity_df = pd.DataFrame({
    "driver_id": [1001, 1002, 1003],
    "event_timestamp": [datetime(2023, 5, 1)] * 3,
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
).to_df()
```

### Online (real-time serving)
```python
store = FeatureStore(repo_path=".")

feature_vector = store.get_online_features(
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
    ],
    entity_rows=[{"driver_id": 1001}],
).to_dict()
```

## CLI Reference

| Command | Description |
|---------|-------------|
| `feast init [NAME]` | Create a new feature repository |
| `feast apply` | Register feature definitions |
| `feast teardown` | Remove all infrastructure |
| `feast materialize START END` | Load features into online store |
| `feast materialize-incremental END` | Incremental materialization |
| `feast ui` | Launch web UI |
| `feast serve` | Start online feature server |
| `feast permissions list` | List access control rules |
| `feast registry-dump` | Dump registry contents |

## Supported Infrastructure

**Online Stores**: SQLite, Redis, DynamoDB, Datastore, PostgreSQL, Cassandra, MySQL, Hazelcast, IKV

**Offline Stores**: File (Parquet/Delta), BigQuery, Snowflake, Redshift, Spark, Trino, PostgreSQL, MSSQL, Clickhouse

**Registries**: Local file, S3, GCS, Azure Blob, PostgreSQL, MySQL, Snowflake
