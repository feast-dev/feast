# Spark source (contrib)

## Description

Spark data sources are tables or files that can be loaded from some Spark store (e.g. Hive or in-memory). They can also be specified by a SQL query.

**New in Feast:** SparkSource now supports advanced table formats including **Apache Iceberg**, **Delta Lake**, and **Apache Hudi**, enabling ACID transactions, time travel, and schema evolution capabilities.

## Disclaimer

The Spark data source does not achieve full test coverage.
Please do not assume complete stability.

## Examples

### Basic Examples

Using a table reference from SparkSession (for example, either in-memory or a Hive Metastore):

```python
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)

my_spark_source = SparkSource(
    table="FEATURE_TABLE",
)
```

Using a query:

```python
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)

my_spark_source = SparkSource(
    query="SELECT timestamp as ts, created, f1, f2 "
          "FROM spark_table",
)
```

Using a file reference:

```python
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)

my_spark_source = SparkSource(
    path=f"{CURRENT_DIR}/data/driver_hourly_stats",
    file_format="parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)
```

### Table Format Support

SparkSource now supports advanced table formats for modern data lakehouse architectures:

#### Apache Iceberg

```python
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast.table_format import IcebergFormat

# Basic Iceberg configuration
iceberg_format = IcebergFormat(
    catalog="my_catalog",
    namespace="my_database"
)

my_spark_source = SparkSource(
    name="user_features",
    path="my_catalog.my_database.user_table",
    table_format=iceberg_format,
    timestamp_field="event_timestamp"
)
```

Time travel with Iceberg:

```python
# Read from a specific snapshot
iceberg_format = IcebergFormat(
    catalog="spark_catalog",
    namespace="lakehouse"
)
iceberg_format.set_property("snapshot-id", "123456789")

my_spark_source = SparkSource(
    name="historical_features",
    path="spark_catalog.lakehouse.features",
    table_format=iceberg_format,
    timestamp_field="event_timestamp"
)
```

#### Delta Lake

```python
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast.table_format import DeltaFormat

# Basic Delta configuration
delta_format = DeltaFormat()

my_spark_source = SparkSource(
    name="transaction_features",
    path="s3://my-bucket/delta-tables/transactions",
    table_format=delta_format,
    timestamp_field="transaction_timestamp"
)
```

Time travel with Delta:

```python
# Read from a specific version
delta_format = DeltaFormat()
delta_format.set_property("versionAsOf", "5")

my_spark_source = SparkSource(
    name="historical_transactions",
    path="s3://my-bucket/delta-tables/transactions",
    table_format=delta_format,
    timestamp_field="transaction_timestamp"
)
```

#### Apache Hudi

```python
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import SparkSource
from feast.table_format import HudiFormat

# Basic Hudi configuration
hudi_format = HudiFormat(
    table_type="COPY_ON_WRITE",  # or "MERGE_ON_READ"
    record_key="user_id",
    precombine_field="updated_at"
)

my_spark_source = SparkSource(
    name="user_profiles",
    path="s3://my-bucket/hudi-tables/user_profiles",
    table_format=hudi_format,
    timestamp_field="event_timestamp"
)
```

## Configuration Options

The full set of configuration options is available [here](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.spark_offline_store.spark_source.SparkSource).

### Table Format Options

- **IcebergFormat**: See [Python API reference](https://rtd.feast.dev/en/master/#feast.table_format.IcebergFormat)
- **DeltaFormat**: See [Python API reference](https://rtd.feast.dev/en/master/#feast.table_format.DeltaFormat)
- **HudiFormat**: See [Python API reference](https://rtd.feast.dev/en/master/#feast.table_format.HudiFormat)

## Supported Types

Spark data sources support all eight primitive types and their corresponding array types.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).
