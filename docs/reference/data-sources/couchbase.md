# Couchbase Columnar source (contrib)

## Description

Couchbase Columnar data sources are [Couchbase Capella Columnar](https://docs.couchbase.com/columnar/intro/intro.html) collections that can be used as a source for feature data. **Note that Couchbase Columnar is available through [Couchbase Capella](https://cloud.couchbase.com/).**

## Disclaimer

The Couchbase Columnar data source does not achieve full test coverage.
Please do not assume complete stability.

## Examples

Defining a Couchbase Columnar source:

```python
from feast.infra.offline_stores.contrib.couchbase_offline_store.couchbase_source import (
    CouchbaseColumnarSource,
)

driver_stats_source = CouchbaseColumnarSource(
    name="driver_hourly_stats_source",
    query="SELECT * FROM Default.Default.`feast_driver_hourly_stats`",
    database="Default",
    scope="Default",
    collection="feast_driver_hourly_stats",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)
```

The full set of configuration options is available [here](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.contrib.couchbase_offline_store.couchbase_source.CouchbaseColumnarSource).

## Supported Types

Couchbase Capella Columnar data sources support `BOOLEAN`, `STRING`, `BIGINT`, and `DOUBLE` primitive types.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).
