# MariaBD source (contrib)

## Description

MariaDB data sources are MariaDB table sources.
These can be specified either by a table reference or a SQL query.

## Disclaimer

The MariaDB data source does not achieve full test coverage.
Please do not assume complete stability.

## Examples

Defining a MariaDB source:

```python
from feast.infra.offline_stores.contrib.mariadb_offline_store.mariadb_source import (
    MariaDBSource,
)

driver_hourly_table = "driver_hourly"

driver_source = MariaDBSource(
    table_ref=driver_hourly_table,
    event_timestamp_column="datetime",
    created_timestamp_column="created",
)
```
