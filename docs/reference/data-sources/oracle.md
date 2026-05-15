# Oracle source (contrib)

## Description

Oracle data sources are Oracle database tables.
These are specified by a table reference (e.g. `"TRANSACTION_FEATURES"` or `"SCHEMA.TABLE"`).

## Disclaimer

The Oracle data source does not achieve full test coverage.
Please do not assume complete stability.

## Examples

Defining an Oracle source:

```python
from feast.infra.offline_stores.contrib.oracle_offline_store.oracle_source import (
    OracleSource,
)

driver_stats_source = OracleSource(
    name="driver_hourly_stats",
    table_ref="DRIVER_HOURLY_STATS",
    event_timestamp_column="EVENT_TIMESTAMP",
    created_timestamp_column="CREATED",
)
```

**Note:** Oracle stores unquoted identifiers in uppercase. Reference columns using the casing shown by Oracle (e.g. `USER_ID` for unquoted identifiers).

## Supported Types

Oracle data sources support standard Oracle numeric, string, date, and timestamp types mapped through the ibis Oracle backend.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).

