# Spark

## Description

**NOTE**: Spark data source api is currently in alpha development and the API is not completely stable. The API may change or update in the future.

The spark data source API allows for the retrieval of historical feature values from file/database sources for building training datasets as well as materializing features into an online store.

* Either a table name, a SQL query, or a file path can be provided.

## Examples

Using a table reference from SparkSession(for example, either in memory or a Hive Metastore)

```python
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)

my_spark_source = SparkSource(
    table="FEATURE_TABLE",
)
```

Using a query

```python
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)

my_spark_source = SparkSource(
    query="SELECT timestamp as ts, created, f1, f2 "
          "FROM spark_table",
)
```

Using a file reference

```python
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)

my_spark_source = SparkSource(
    path=f"{CURRENT_DIR}/data/driver_hourly_stats",
    file_format="parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
)
```
