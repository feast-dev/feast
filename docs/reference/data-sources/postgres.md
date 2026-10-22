# PostgreSQL source (contrib)

## Description

PostgreSQL data sources are PostgreSQL tables or views.
These can be specified either by a table reference or a SQL query.

## Disclaimer

The PostgreSQL data source does not achieve full test coverage.
Please do not assume complete stability.

## Examples

Defining a Postgres source:

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

The full set of configuration options is available [here](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source.PostgreSQLSource).

## Supported Types

PostgreSQL data sources support all eight primitive types and their corresponding array types.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).
