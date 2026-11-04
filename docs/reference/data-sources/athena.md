# Athena source (contrib)

## Description

Athena data sources are AWS Athena tables or views.
These can be specified either by a table reference or a SQL query.

## Disclaimer

The Athena data source does not achieve full test coverage.
Please do not assume complete stability.

## Examples

Defining an Athena source:

```python
from feast.infra.offline_stores.contrib.athena_offline_store.athena_source import (
    AthenaSource,
)

driver_stats_source = AthenaSource(
    name="driver_hourly_stats",
    table="driver_hourly_stats",
    database="my_database",
    data_source="AwsDataCatalog",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)
```

The full set of configuration options is available [here](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.athena_offline_store.athena_source.AthenaSource).

## Supported Types

Athena data sources support standard Athena types mapped through the AWS Athena API.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).
