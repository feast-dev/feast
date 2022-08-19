# MsSQL source (contrib)

## Description

MsSQL data sources are Microsoft sql table sources.
These can be specified either by a table reference or a SQL query.

## Disclaimer

The MsSQL data source does not achieve full test coverage.
Please do not assume complete stability.

## Examples

Defining a MsSQL source:

```python
from feast.infra.offline_stores.contrib.mssql_offline_store.mssqlserver_source import (
    MsSqlServerSource,
)

driver_hourly_table = "driver_hourly"

driver_source = MsSqlServerSource(
    table_ref=driver_hourly_table,
    event_timestamp_column="datetime",
    created_timestamp_column="created",
)
```
