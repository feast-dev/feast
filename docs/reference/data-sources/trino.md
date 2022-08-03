# Trino source (contrib)

## Description

Trino data sources are Trino tables or views.
These can be specified either by a table reference or a SQL query.

## Disclaimer

The Trino data source does not achieve full test coverage.
Please do not assume complete stability. 

## Examples

Defining a Trino source:

```python
from feast.infra.offline_stores.contrib.trino_offline_store.trino_source import (
    TrinoSource,
)

driver_hourly_stats = TrinoSource(
    event_timestamp_column="event_timestamp",
    table_ref="feast.driver_stats",
    created_timestamp_column="created",
)
```

The full set of configuration options is available [here](https://rtd.feast.dev/en/master/#trino-source).
