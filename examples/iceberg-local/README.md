# Iceberg Local Example

This example demonstrates how to use Feast with Apache Iceberg as both offline and online storage, running entirely on your local machine.

## Overview

This example shows:
- **Local Iceberg setup** with SQLite catalog and filesystem storage
- **Offline store** for historical feature retrieval (point-in-time correct)
- **Online store** for low-latency feature serving
- **Complete workflow** from data generation to feature retrieval

## Prerequisites

- Python 3.10+ (but <3.13 for PyArrow compatibility)
- feast with Iceberg extras installed

## Installation

```bash
# Install Feast with Iceberg support
pip install feast[iceberg]

# Or using uv (recommended)
uv pip install feast[iceberg]
```

## Project Structure

```
iceberg-local/
├── feature_store.yaml   # Feast configuration
├── features.py          # Feature definitions
├── run_example.py       # Complete workflow script
├── data/                # Generated data directory (created on first run)
│   ├── iceberg_catalog.db          # SQLite catalog for offline store
│   ├── iceberg_online_catalog.db   # SQLite catalog for online store
│   ├── warehouse/                   # Offline Iceberg tables
│   ├── online_warehouse/            # Online Iceberg tables
│   └── registry.db                  # Feast registry
└── README.md            # This file
```

## Quick Start

Run the complete example:

```bash
python run_example.py
```

This script will:
1. Create sample driver statistics data (7 days, 5 drivers, hourly data)
2. Write data to an Iceberg table using PyIceberg
3. Apply feature definitions to Feast
4. Materialize features to the online store
5. Retrieve online features for real-time serving
6. Retrieve historical features with point-in-time correctness

## Step-by-Step Walkthrough

### 1. Configuration (`feature_store.yaml`)

The configuration uses:
- **SQLite catalogs** for both offline and online stores (no external dependencies)
- **Local filesystem** for Iceberg table storage
- **Entity hash partitioning** for the online store

```yaml
offline_store:
    type: iceberg
    catalog_type: sql
    uri: sqlite:///data/iceberg_catalog.db
    warehouse: data/warehouse

online_store:
    type: iceberg
    catalog_type: sql
    uri: sqlite:///data/iceberg_online_catalog.db
    warehouse: data/online_warehouse
    partition_strategy: entity_hash
```

### 2. Feature Definitions (`features.py`)

Defines:
- **Entity**: `driver` (with `driver_id` join key)
- **Data Source**: `IcebergSource` pointing to `demo.driver_stats` table
- **Feature View**: `driver_hourly_stats` with 3 features (conv_rate, acc_rate, avg_daily_trips)
- **Feature Services**: Grouped features for specific models

### 3. Running the Workflow

The `run_example.py` script demonstrates:

#### a) Create Sample Data
```python
df = create_sample_data()  # 7 days of hourly data for 5 drivers
```

#### b) Write to Iceberg
```python
setup_iceberg_table(df)  # Create table and write with PyIceberg
```

#### c) Apply Features
```python
fs = FeatureStore(repo_path=".")
fs.apply(["features.py"])
```

#### d) Materialize to Online Store
```python
fs.materialize(start_date, end_date)
```

#### e) Get Online Features
```python
online_features = fs.get_online_features(
    features=["driver_hourly_stats:conv_rate"],
    entity_rows=[{"driver_id": 1001}],
)
```

#### f) Get Historical Features
```python
training_df = fs.get_historical_features(
    entity_df=entity_df,  # DataFrame with driver_id and event_timestamp
    features=["driver_hourly_stats:conv_rate"],
)
```

## Understanding the Output

### Online Features
Returns the **latest** feature values for each entity:

```
   driver_id  conv_rate  acc_rate  avg_daily_trips
0       1001   0.523456  0.812345                12
1       1002   0.534567  0.823456                13
2       1003   0.545678  0.834567                14
```

### Historical Features
Returns **point-in-time correct** feature values (no data leakage):

```
   driver_id        event_timestamp  conv_rate  acc_rate  avg_daily_trips
0       1001 2025-01-13 18:00:00      0.520000  0.810000                12
1       1002 2025-01-12 18:00:00      0.531000  0.821000                13
2       1003 2025-01-11 18:00:00      0.542000  0.832000                14
```

For each row, features are retrieved from data available **before** the event_timestamp.

## Exploring the Data

After running the example, you can inspect the generated files:

```bash
# Check Iceberg catalog database
sqlite3 data/iceberg_catalog.db "SELECT * FROM iceberg_tables;"

# List warehouse files
ls -lah data/warehouse/demo.db/driver_stats/

# Check online warehouse
ls -lah data/online_warehouse/online.db/
```

## Advanced Usage

### Custom Data

Modify `create_sample_data()` in `run_example.py` to use your own data:

```python
def create_sample_data() -> pd.DataFrame:
    # Load your data
    df = pd.read_csv("my_data.csv")
    return df
```

### Add New Features

Edit `features.py` to add new features:

```python
Field(
    name="total_earnings",
    dtype=Float32,
    description="Total earnings for the driver",
),
```

### Change Partition Strategy

Edit `feature_store.yaml` to try different partitioning:

```yaml
online_store:
    partition_strategy: timestamp  # or "hybrid"
```

Options:
- `entity_hash`: Partition by hash of entity values (default, best for balanced distribution)
- `timestamp`: Partition by event timestamp (best for time-series queries)
- `hybrid`: Combine both strategies

## Production Deployment

To move from local to production:

### 1. Use Cloud Storage

Replace SQLite catalog with REST or Hive catalog:

```yaml
offline_store:
    type: iceberg
    catalog_type: rest
    uri: https://your-iceberg-catalog.com
    warehouse: s3://your-bucket/warehouse
    storage_options:
        s3.region: us-west-2
```

### 2. Use Cloudflare R2 (S3-compatible)

```yaml
offline_store:
    type: iceberg
    catalog_type: sql
    uri: postgresql://user:pass@host:5432/catalog
    warehouse: s3://my-bucket/warehouse
    storage_options:
        s3.endpoint: https://<account-id>.r2.cloudflarestorage.com
        s3.access-key-id: ${R2_ACCESS_KEY_ID}
        s3.secret-access-key: ${R2_SECRET_ACCESS_KEY}
        s3.region: auto
        s3.force-virtual-addressing: true
```

### 3. Scale the Online Store

For production workloads, consider:
- Separate online catalog from offline
- Use distributed query engine (Spark, Dask) for offline queries
- Optimize partition count for your entity cardinality
- Monitor partition size and rebalance if needed

## Troubleshooting

### PyArrow version conflicts
```bash
# Ensure Python < 3.13
python --version

# Reinstall with explicit PyArrow version
pip install pyarrow==15.0.0 feast[iceberg]
```

### Catalog errors
```bash
# Remove and recreate catalog
rm -rf data/
python run_example.py
```

### Import errors
```bash
# Ensure you're in the example directory
cd examples/iceberg-local
python run_example.py
```

## Next Steps

- Explore [Feast documentation](https://docs.feast.dev)
- Learn about [Apache Iceberg](https://iceberg.apache.org)
- Try [Cloudflare R2](https://developers.cloudflare.com/r2) for production storage
- Integrate with your ML pipeline

## Support

- [Feast Slack](https://feast-slack.herokuapp.com)
- [GitHub Issues](https://github.com/feast-dev/feast/issues)
- [Documentation](https://docs.feast.dev)
