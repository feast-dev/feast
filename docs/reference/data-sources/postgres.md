# PostgreSQL

## Description

**NOTE**: The Postgres plugin is a contrib plugin. This means it may not be fully stable.


The PostgreSQL data source allows for the retrieval of historical feature values from a PostgreSQL database for building training datasets as well as materializing features into an online store.

## Examples

Defining a Postgres source

```python
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgreSQLSource,
)

driver_stats_source = PostgreSQLSource(
    name="feast_driver_hourly_stats",
    query="SELECT * FROM feast_driver_hourly_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)
```
