# Clickhouse source (contrib)

## Description

Clickhouse data sources are Clickhouse tables or views.
These can be specified either by a table reference or a SQL query.

## Disclaimer

The Clickhouse data source does not achieve full test coverage.
Please do not assume complete stability.

## Examples

Defining a Clickhouse source:

```python
from feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source import (
    ClickhouseSource,
)

driver_stats_source = ClickhouseSource(
    name="feast_driver_hourly_stats",
    query="SELECT * FROM feast_driver_hourly_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)
```

The full set of configuration options is available [here](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.clickhouse_offline_store.clickhouse_source.ClickhouseSource).

## Supported Types

Clickhouse data sources support all eight primitive types and their corresponding array types.
The support for Clickhouse Decimal type is achieved by converting it to double.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).
