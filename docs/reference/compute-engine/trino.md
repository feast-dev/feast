# Trino

## Description

Trino Compute Engine provides a distributed execution engine for batch materialization operations (`materialize` and `materialize-incremental`) and historical retrieval operations (`get_historical_features`).

It is designed to handle large-scale data processing directly on Trino clusters without moving raw data to the client machine.

### Design

The Trino Compute engine is implemented as a subclass of `feast.infra.compute_engines.base.ComputeEngine`.

The engine supports the following features:
- **Pushdown SQL execution**: Compiles feature pipeline operations (filtering, deduplication, time-windowed aggregations, point-in-time joins, and transformations) into Trino SQL Common Table Expressions (CTEs) executed directly on the Trino cluster.
- **Streaming online materialization**: Streams query results in chunks as PyArrow record batches directly to the online store via a thread pool, avoiding loading entire datasets into client memory.
- **Atomic offline table writes**: Materializes offline tables using a temporary staging table and rename swap (`CREATE TABLE ...__staging AS ...; DROP TABLE ...; ALTER TABLE ...__staging RENAME TO ...`).
- **Configuration inheritance**: Inherits connection details from the Trino offline store when both are configured.

---

## Example

```yaml
project: feast_trino_project
registry: data/registry.db
provider: local

offline_store:
  type: trino.offline
  host: localhost
  port: 8080
  catalog: iceberg
  dataset: feast_offline
  user: feast_user

batch_engine:
  type: trino.engine
  batch_size: 10000
  write_concurrency: 4

online_store:
  type: redis
  connection_string: localhost:6379
```

---

## Example in Python

```python
from datetime import timedelta
from feast import (
    BatchFeatureView,
    Entity,
    Field,
)
from feast.aggregation import Aggregation
from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import TrinoSource
from feast.transformation.mode import TransformationMode
from feast.transformation.trino_transformation import TrinoTransformation
from feast.types import Float32, Int32
from feast.value_type import ValueType

# 1. Define Entity
driver = Entity(
    name="driver_id",
    value_type=ValueType.INT32,
    join_keys=["driver_id"],
    description="Driver identifier",
)

# 2. Define Trino Batch Source
driver_stats_source = TrinoSource(
    name="driver_stats_source",
    table="iceberg.feast_offline.driver_hourly_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

# 3. Define Pure Trino SQL Transformation
# The '{}' placeholder will be substituted with the upstream CTE/table name
transform = TrinoTransformation(
    mode=TransformationMode.TRINO_SQL,
    udf="""
    SELECT
        driver_id,
        event_timestamp,
        conv_rate * 2.0 AS conv_rate,
        acc_rate * 2.0 AS acc_rate
    FROM {}
    """,
)

# 4. Define Feature View with SQL Transformation & Aggregations
driver_hourly_stats_fv = BatchFeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=3),
    feature_transformation=transform,
    aggregations=[
        Aggregation(column="conv_rate", function="sum"),
        Aggregation(column="acc_rate", function="avg"),
    ],
    schema=[
        Field(name="sum_conv_rate", dtype=Float32),
        Field(name="avg_acc_rate", dtype=Float32),
        Field(name="driver_id", dtype=Int32),
    ],
    online=True,
    offline=True,
    source=driver_stats_source,
)
```

---

## Retrieving and Materializing Features

```python
import pandas as pd
from datetime import datetime, timezone
from feast import FeatureStore

fs = FeatureStore(repo_path=".")

entity_df: pd.DataFrame

# Lazy Historical Retrieval:
# Compiles into a single Trino ANSI SQL query with CTEs.
job = fs.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:sum_conv_rate",
        "driver_hourly_stats:avg_acc_rate",
    ],
)

# Inspect the compiled pure SQL query:
print(job.to_sql())

# Execute on cluster and return pandas dataframe:
training_df = job.to_df()

# Materialize to Online and Offline Stores:
fs.materialize(
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    end_date=datetime(2025, 1, 15, tzinfo=timezone.utc),
)
```
