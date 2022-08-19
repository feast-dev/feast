# BigQuery source

## Description

BigQuery data sources are BigQuery tables or views.
These can be specified either by a table reference or a SQL query.
However, no performance guarantees can be provided for SQL query-based sources, so table references are recommended.

## Examples

Using a table reference:

```python
from feast import BigQuerySource

my_bigquery_source = BigQuerySource(
    table_ref="gcp_project:bq_dataset.bq_table",
)
```

Using a query:

```python
from feast import BigQuerySource

BigQuerySource(
    query="SELECT timestamp as ts, created, f1, f2 "
          "FROM `my_project.my_dataset.my_features`",
)
```

The full set of configuration options is available [here](https://rtd.feast.dev/en/latest/index.html#feast.infra.offline_stores.bigquery_source.BigQuerySource).

## Supported Types

BigQuery data sources support all eight primitive types and their corresponding array types.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).
