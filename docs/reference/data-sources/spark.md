# Spark source (contrib)

## Description

Spark data sources are tables or files that can be loaded from some Spark store (e.g. Hive or in-memory). They can also be specified by a SQL query.

## Disclaimer

The Spark data source does not achieve full test coverage.
Please do not assume complete stability.

## Examples

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

The full set of configuration options is available [here](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.spark_offline_store.spark_source.SparkSource).

## Supported Types

Spark data sources support all eight primitive types and their corresponding array types.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).
