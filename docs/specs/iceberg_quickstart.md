# Iceberg Quickstart Guide

This guide will help you get started with Apache Iceberg as both an offline and online store in Feast.

## Overview

Apache Iceberg provides a modern table format with:
- ACID transactions
- Schema evolution
- Time travel
- Partition evolution
- Hidden partitioning

With Feast's Iceberg integration, you can use the same table format for both offline feature engineering and online serving.

## Prerequisites

* Python 3.10, 3.11, or 3.12
* UV package manager (recommended) or pip
* Docker (optional, for running Iceberg REST catalog)

## Installation

### Using UV (Recommended)

```bash
# Install Feast with Iceberg support
uv sync --extra iceberg

# Or add to an existing project
uv add "feast[iceberg]"
```

### Using pip

```bash
pip install "feast[iceberg]"
```

This installs:
- `pyiceberg[sql,duckdb]>=0.8.0` - Native Iceberg support
- `duckdb>=1.0.0` - SQL engine for joins

## Option 1: Local Development (SQL Catalog)

Perfect for getting started quickly without external dependencies.

### 1. Create Feature Repository

```bash
# Create a new Feast repository
mkdir iceberg_feast && cd iceberg_feast
uv run feast init iceberg_demo
cd iceberg_demo
```

### 2. Configure Iceberg Stores

Edit `feature_store.yaml`:

```yaml
project: iceberg_demo
registry: data/registry.db
provider: local

# Offline store for feature engineering
offline_store:
    type: feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.IcebergOfflineStore
    catalog_type: sql
    catalog_name: feast_catalog
    uri: sqlite:///data/iceberg_catalog.db
    warehouse: data/warehouse
    namespace: feast

# Online store for feature serving
online_store:
    type: iceberg
    catalog_type: sql
    catalog_name: feast_catalog
    uri: sqlite:///data/iceberg_catalog.db
    warehouse: data/warehouse
    namespace: feast_online
    partition_strategy: entity_hash
    partition_count: 16  # Small for local dev
```

### 3. Define Features

Create `features.py`:

```python
from datetime import timedelta
from feast import Entity, FeatureView, Field
from feast.types import Float32, Int64
from feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg_source import (
    IcebergSource,
)

# Define entity
driver = Entity(
    name="driver",
    join_keys=["driver_id"],
)

# Define Iceberg data source
driver_stats_source = IcebergSource(
    name="driver_hourly_stats",
    table="feast.driver_hourly_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

# Define feature view
driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    source=driver_stats_source,
    ttl=timedelta(days=1),
)
```

### 4. Apply Configuration

```bash
# Apply feature definitions
uv run feast apply
```

### 5. Generate Sample Data

Create `generate_data.py`:

```python
import pandas as pd
from datetime import datetime, timedelta
from pyiceberg.catalog import load_catalog

# Create sample data
n_drivers = 100
n_days = 7
dates = [datetime.now() - timedelta(days=i) for i in range(n_days)]

data = []
for driver_id in range(1, n_drivers + 1):
    for date in dates:
        data.append({
            "driver_id": driver_id,
            "event_timestamp": date,
            "created": datetime.now(),
            "conv_rate": 0.5 + (driver_id % 10) * 0.05,
            "acc_rate": 0.8 + (driver_id % 5) * 0.03,
            "avg_daily_trips": 10 + (driver_id % 20),
        })

df = pd.DataFrame(data)

# Load Iceberg catalog
catalog = load_catalog(
    "feast_catalog",
    type="sql",
    uri="sqlite:///data/iceberg_catalog.db",
    warehouse="data/warehouse",
)

# Create namespace
try:
    catalog.create_namespace("feast")
except:
    pass

# Write to Iceberg table
import pyarrow as pa
arrow_table = pa.Table.from_pandas(df)
catalog.create_table(
    identifier="feast.driver_hourly_stats",
    schema=arrow_table.schema,
)
table = catalog.load_table("feast.driver_hourly_stats")
table.append(arrow_table)

print(f"✅ Created table with {len(df)} rows")
```

Run it:
```bash
uv run python generate_data.py
```

### 6. Materialize Features

```bash
# Materialize features to online store
uv run feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
```

### 7. Retrieve Features

Create `retrieve_features.py`:

```python
from feast import FeatureStore
import pandas as pd

# Initialize feature store
store = FeatureStore(repo_path=".")

# Entity dataframe
entity_df = pd.DataFrame({
    "driver_id": [1, 2, 3, 4, 5],
})

# Get online features
features = store.get_online_features(
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
    entity_rows=entity_df.to_dict("records"),
).to_df()

print(features)
```

Run it:
```bash
uv run python retrieve_features.py
```

## Option 2: Production Setup (REST Catalog + S3)

For production deployments with distributed storage.

### 1. Start Iceberg REST Catalog

```bash
# Using Docker
docker run -p 8181:8181 \
  -e AWS_ACCESS_KEY_ID=minio \
  -e AWS_SECRET_ACCESS_KEY=minio123 \
  -e AWS_REGION=us-east-1 \
  -e CATALOG_WAREHOUSE=s3://warehouse/ \
  -e CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO \
  -e CATALOG_S3_ENDPOINT=http://minio:9000 \
  tabulario/iceberg-rest:latest
```

### 2. Configure Feature Store

```yaml
project: iceberg_prod
registry: s3://feast-registry/registry.db
provider: aws

offline_store:
    type: feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.IcebergOfflineStore
    catalog_type: rest
    catalog_name: feast_catalog
    uri: http://iceberg-rest:8181
    warehouse: s3://data-lake/warehouse
    namespace: feast
    storage_options:
        s3.region: us-east-1
        s3.access-key-id: ${AWS_ACCESS_KEY_ID}
        s3.secret-access-key: ${AWS_SECRET_ACCESS_KEY}

online_store:
    type: iceberg
    catalog_type: rest
    catalog_name: feast_catalog
    uri: http://iceberg-rest:8181
    warehouse: s3://data-lake/warehouse
    namespace: feast_online
    partition_strategy: entity_hash
    partition_count: 256
    storage_options:
        s3.region: us-east-1
        s3.access-key-id: ${AWS_ACCESS_KEY_ID}
        s3.secret-access-key: ${AWS_SECRET_ACCESS_KEY}
```

### 3. Use AWS Glue Catalog (Alternative)

```yaml
offline_store:
    type: feast.infra.offline_stores.contrib.iceberg_offline_store.iceberg.IcebergOfflineStore
    catalog_type: glue
    catalog_name: feast_catalog
    warehouse: s3://data-lake/warehouse
    namespace: feast
    storage_options:
        s3.region: us-west-2

online_store:
    type: iceberg
    catalog_type: glue
    catalog_name: feast_catalog
    warehouse: s3://data-lake/warehouse
    namespace: feast_online
    partition_strategy: entity_hash
    partition_count: 256
    storage_options:
        s3.region: us-west-2
```

## Common Workflows

### Historical Features (Training Data)

```python
from feast import FeatureStore
import pandas as pd
from datetime import datetime, timedelta

store = FeatureStore(repo_path=".")

# Entity dataframe with timestamps
entity_df = pd.DataFrame({
    "driver_id": [1, 2, 3, 4, 5],
    "event_timestamp": [
        datetime.now() - timedelta(days=i)
        for i in range(5)
    ],
})

# Get point-in-time correct features
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
    ],
).to_df()

print(training_df)
```

### Online Features (Inference)

```python
from feast import FeatureStore

store = FeatureStore(repo_path=".")

# Get latest features for entities
online_features = store.get_online_features(
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
    ],
    entity_rows=[
        {"driver_id": 1},
        {"driver_id": 2},
    ],
).to_dict()

print(online_features)
```

### Batch Scoring

```python
import pandas as pd

# Load large entity dataset
entity_df = pd.read_parquet("s3://data/entities/batch_20240115.parquet")

# Get features for all entities
batch_features = store.get_historical_features(
    entity_df=entity_df,
    features=["driver_hourly_stats:*"],
).to_df()

# Save for scoring
batch_features.to_parquet("s3://data/features/batch_20240115.parquet")
```

## Performance Tuning

### Partition Strategy Selection

```yaml
# High-cardinality entities (millions): entity_hash with 256+ partitions
partition_strategy: entity_hash
partition_count: 256

# Time-series workloads: timestamp partitioning
partition_strategy: timestamp

# Mixed workloads: hybrid partitioning
partition_strategy: hybrid
partition_count: 64
```

### Materialization Schedule

```bash
# Hourly materialization (cron job)
0 * * * * cd /path/to/repo && uv run feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")

# Daily materialization
0 0 * * * cd /path/to/repo && uv run feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")
```

## Monitoring

### Check Table Metadata

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog(
    "feast_catalog",
    type="rest",
    uri="http://localhost:8181",
)

table = catalog.load_table("feast_online.project_driver_hourly_stats_online")
print(f"Snapshots: {len(table.metadata.snapshots)}")
print(f"Schema: {table.schema()}")
```

### Monitor Storage

```bash
# Check S3 storage size
aws s3 ls s3://data-lake/warehouse/feast_online/ --recursive --summarize

# Check file count
aws s3 ls s3://data-lake/warehouse/feast_online/ --recursive | wc -l
```

## Troubleshooting

### Issue: "Table not found"

```bash
# Verify catalog connection
uv run python -c "from pyiceberg.catalog import load_catalog; catalog = load_catalog('feast_catalog', type='sql', uri='sqlite:///data/iceberg_catalog.db'); print(catalog.list_tables('feast'))"
```

### Issue: "Slow online reads"

Check partition pruning:
```python
# Ensure partition_count matches entity cardinality
# More partitions = better pruning for high-cardinality entities
```

### Issue: "Write failures"

Check permissions:
```bash
# S3 permissions
aws s3 ls s3://data-lake/warehouse/

# Catalog connectivity
curl http://localhost:8181/v1/config
```

## Next Steps

* **Scale Up**: Move from SQL catalog to REST catalog or AWS Glue
* **Optimize**: Tune partition strategy based on access patterns
* **Monitor**: Set up CloudWatch/Prometheus metrics
* **Secure**: Use IAM roles instead of access keys
* **Automate**: Schedule materialization with Airflow/cron

## Resources

* [Iceberg Offline Store Reference](../reference/offline-stores/iceberg.md)
* [Iceberg Online Store Reference](../reference/online-stores/iceberg.md)
* [Apache Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
* [PyIceberg Documentation](https://py.iceberg.apache.org/)
