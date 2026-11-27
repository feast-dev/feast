---
title: "Feast Gets Native Apache Iceberg Support"
description: "Feast now reads features from any Iceberg catalog — REST, SQL, Hive, Glue, DynamoDB. Connect to Unity Catalog, Apache Polaris, Nessie, or your own PyIceberg catalog. Full support for get_historical_features, materialize, and online serving."
date: 2026-07-18
authors: ["Nikhil Kathole"]
---

# Feast Gets Native Apache Iceberg Support

Apache Iceberg has become the open table format. Your data lake is probably already on it — whether through Databricks, Snowflake, AWS, or self-managed infrastructure. But until now, connecting Feast to Iceberg tables meant either going through Spark (heavyweight, slow to start) or copying data into Feast-managed Parquet files (data duplication, governance gap).

Feast now ships a native `IcebergSource` that reads directly from any Iceberg catalog. No data copies. Your feature tables live where they already live — in your Iceberg catalog — and Feast reads from them via PyIceberg. With the DuckDB offline store, you don't even need a Spark cluster — reads happen entirely in-process.

## Why This Matters

Before this, the path from "data in Iceberg" to "features in Feast" looked like this:

1. Data engineers build Iceberg tables in their catalog (UC, Glue, Hive)
2. ML engineers copy data to Feast-managed Parquet files, or configure a SparkSource that couples them to a specific compute engine
3. Two copies of the data. Two metadata systems. No connection between them.

Now the path is:

1. Data engineers build Iceberg tables in their catalog
2. ML engineers point `IcebergSource` at the table
3. Done. Feast reads directly from the catalog via PyIceberg. One copy. One source of truth. Choose DuckDB for lightweight local reads or Spark when you need distributed compute — the data source definition stays the same either way.

## What You Get

### Any Iceberg Catalog

`IcebergSource` supports every catalog backend that PyIceberg supports:

| `catalog_type` | Backend | Example Use Case |
|---|---|---|
| `"rest"` | Iceberg REST Catalog | Databricks Unity Catalog, Apache Polaris, Project Nessie, Snowflake Open Catalog |
| `"sql"` | SQL-backed catalog | Local dev with SQLite, CI/CD, PostgreSQL-backed catalogs |
| `"hive"` | Hive Metastore | On-premise Hadoop, EMR |
| `"glue"` | AWS Glue Data Catalog | AWS-native lakehouse |
| `"dynamodb"` | DynamoDB catalog | Serverless AWS |

### Both Offline Stores

| Operation | DuckDB | Spark |
|---|---|---|
| `feast apply` | Yes | Yes |
| `get_historical_features` | Yes | Yes |
| `materialize` / `materialize-incremental` | Yes | Yes |
| `get_online_features` | Yes | Yes |

Both offline stores use PyIceberg for the actual Iceberg table scan — the difference is what happens after. DuckDB processes the Arrow table in-process (no JVM, no cluster), making it ideal for local development and moderate-scale workloads. Spark is there when you need distributed compute over large datasets. The same `IcebergSource` definition works with either offline store — just change `offline_store.type` in your YAML.

### Full Iceberg Semantics

Every read goes through PyIceberg's `table.scan().to_arrow()`. This means you get proper Iceberg semantics: schema evolution, partition pruning, and snapshot isolation — not just raw Parquet file reads.

## Quick Start

### Install

```bash
pip install "feast[iceberg]"
```

### Define a Source

```python
from feast.infra.data_sources.contrib.iceberg_catalog import IcebergSource

driver_stats = IcebergSource(
    warehouse="my_catalog",
    namespace="ml_features",
    table="driver_hourly_stats",
    catalog_type="rest",
    endpoint="https://my-iceberg-catalog.example.com",
    token_env_var="CATALOG_TOKEN",
    timestamp_field="event_timestamp",
)
```

### Use It

```python
from datetime import timedelta
from feast import Entity, FeatureView, Field
from feast.types import Float64, Int64

driver = Entity(name="driver", join_keys=["driver_id"])

driver_stats_fv = FeatureView(
    name="driver_hourly_stats",
    entities=[driver],
    ttl=timedelta(days=365),
    schema=[
        Field(name="driver_id", dtype=Int64),
        Field(name="conv_rate", dtype=Float64),
        Field(name="acc_rate", dtype=Float64),
        Field(name="avg_daily_trips", dtype=Int64),
    ],
    source=driver_stats,
    online=True,
)
```

```yaml
# feature_store.yaml
project: my_project
registry: data/registry.db
provider: local
online_store:
  type: sqlite
  path: data/online_store.db
offline_store:
  type: duckdb
```

```bash
feast apply
```

Then use it like any other Feast source:

```python
from feast import FeatureStore
import pandas as pd
from datetime import datetime, timezone

store = FeatureStore(repo_path=".")

# Training data
training_df = store.get_historical_features(
    entity_df=pd.DataFrame({
        "driver_id": [1001, 1002, 1003],
        "event_timestamp": [datetime(2026, 7, 1, tzinfo=timezone.utc)] * 3,
    }),
    features=["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"],
).to_df()

# Materialize to online store
store.materialize_incremental(end_date=datetime.now(tz=timezone.utc))

# Online serving
online = store.get_online_features(
    features=["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"],
    entity_rows=[{"driver_id": 1001}],
).to_dict()
```

## Catalog Examples

### AWS Glue

```python
glue_source = IcebergSource(
    warehouse="my_glue_database",
    namespace="ml_features",
    table="driver_stats",
    catalog_type="glue",
    catalog_properties={"region_name": "us-east-1"},
    timestamp_field="event_timestamp",
)
```

### Hive Metastore

```python
hive_source = IcebergSource(
    endpoint="thrift://hive-metastore:9083",
    warehouse="warehouse",
    namespace="features",
    table="driver_stats",
    catalog_type="hive",
    timestamp_field="event_timestamp",
)
```

### Apache Polaris / Nessie

```python
polaris_source = IcebergSource(
    endpoint="https://polaris.example.com",
    warehouse="my_catalog",
    namespace="ml",
    table="features",
    catalog_type="rest",
    token_env_var="POLARIS_TOKEN",
    timestamp_field="event_timestamp",
)
```

### Local Development (SQLite-backed)

For development and CI/CD, use a local PyIceberg SQL catalog — no external service required:

```python
local_source = IcebergSource(
    warehouse="dev_warehouse",
    namespace="default",
    table="driver_stats",
    catalog_type="sql",
    catalog_name="dev_catalog",
    catalog_properties={
        "uri": "sqlite:////tmp/iceberg_catalog.db",
        "warehouse": "file:///tmp/iceberg_warehouse",
    },
    timestamp_field="event_timestamp",
)
```

## Use Case: Unity Catalog Integration

Unity Catalog users get everything above with simpler configuration. `UnityCatalogSource` extends `IcebergSource` with UC-specific defaults:

- Default connection via `DATABRICKS_HOST` and `DATABRICKS_TOKEN` environment variables — no manual endpoint or token setup
- Three-level naming (`warehouse.namespace.table`) maps directly to UC's catalog/schema/table hierarchy

### Databricks Setup

```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi_your_token_here"
```

```python
from feast.infra.data_sources.contrib.iceberg_catalog import UnityCatalogSource

driver_stats_source = UnityCatalogSource(
    warehouse="ml_catalog",
    namespace="driver_features",
    table="driver_hourly_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
    description="Hourly aggregated driver statistics",
)
```

That's it. No endpoint or token parameters needed — they come from the environment variables.

### Optional: Governance Metadata Sync

If you want `feast apply` to annotate your UC tables with `feast.*` properties (project name, feature view, primary keys, owner), you can use the `UnityCatalogProvider`:

```yaml
# feature_store.yaml
provider: unity_catalog
```

This is optional. For most users, `provider: local` is sufficient. Reads, materialization, and online serving work the same regardless of which provider you use.

### OSS Unity Catalog

The integration also works with the open-source Unity Catalog, with some differences:

| Capability | Databricks UC | OSS UC |
|---|---|---|
| Read via Iceberg REST | Built-in | Requires `uniform_iceberg_metadata_location` in H2 DB |
| Read via SQL catalog | Yes | Yes |
| Credential vending | Yes | Not available |

For OSS UC, set `credential_vending=False` and `token_env_var=None`:

```python
source = UnityCatalogSource(
    warehouse="unity",
    namespace="default",
    table="driver_hourly_stats",
    endpoint="http://localhost:8080/api/2.1/unity-catalog/iceberg",
    token_env_var=None,
    credential_vending=False,
    catalog_type="sql",    # recommended for OSS UC
    catalog_name="my_catalog",
    catalog_properties={
        "uri": "sqlite:////tmp/pyiceberg_catalog.db",
        "warehouse": "file:///tmp/warehouse",
    },
    timestamp_field="event_timestamp",
    register_as_feature_table=False,
)
```

## How It Works

### Read Path

All data reads go through PyIceberg, regardless of catalog type:

```
IcebergSource.get_catalog_client()
    → PyIceberg catalog (REST / SQL / Hive / Glue / DynamoDB)
    → table.scan().to_arrow()
    → Arrow table
    → DuckDB ibis memtable or Spark DataFrame
```

If the catalog is misconfigured, you get a clear error — consistent with how every other Feast data source works.

### Catalog Name Isolation

Each source has a `catalog_name` parameter (default: `"feast_iceberg"`). This is the instance name PyIceberg uses when loading the catalog. If you have multiple sources pointing at different catalogs, use different names to avoid collisions:

```python
production = IcebergSource(catalog_name="prod_catalog", ...)
staging = IcebergSource(catalog_name="staging_catalog", ...)
```

## What's Not Supported

- **Write-back to Iceberg/UC tables.** Feast reads from existing tables; it doesn't write feature data back. The `write_to_offline_store` API only supports `FileSource` (DuckDB) and `SparkSource` (Spark). Your data engineering pipelines create and populate the tables.
- **Table creation.** Tables must exist before Feast can read from them. `feast apply` registers the feature view in Feast's registry, not the table in the catalog.

## Configuration Reference

### IcebergSource

| Parameter | Default | Description |
|---|---|---|
| `warehouse` | *required* | Catalog or warehouse name |
| `namespace` | *required* | Schema or namespace |
| `table` | *required* | Table name |
| `catalog_type` | `"rest"` | Backend: `"rest"`, `"sql"`, `"hive"`, `"glue"`, `"dynamodb"` |
| `catalog_name` | `"feast_iceberg"` | PyIceberg instance name (unique per catalog to avoid collisions) |
| `endpoint` | `None` | Catalog endpoint URL |
| `catalog_properties` | `{}` | Additional catalog config (e.g., `{"uri": "sqlite:///..."}`) |
| `token_env_var` | `None` | Env var containing auth token |
| `credential_vending` | `True` | Request scoped storage credentials |
| `timestamp_field` | `None` | Event timestamp column |
| `created_timestamp_column` | `None` | Creation timestamp for deduplication |

### UnityCatalogSource (extends IcebergSource)

All `IcebergSource` parameters plus:

| Parameter | Default | Description |
|---|---|---|
| `endpoint` | From `DATABRICKS_HOST` | Defaults to `{DATABRICKS_HOST}/api/2.1/unity-catalog/iceberg` |
| `token_env_var` | `"DATABRICKS_TOKEN"` | Defaults to Databricks token env var |
| `register_as_feature_table` | `True` | Sync `feast.*` properties to UC on `feast apply` |
| `sync_lineage` | `True` | Record lineage in UC (Databricks only) |

---

*Native Iceberg support is available in Feast 0.64+. Install with `pip install "feast[iceberg]"` and check the [Iceberg data source documentation](/reference/data-sources/iceberg) for the full API reference.*
