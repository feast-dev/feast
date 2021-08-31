# BigQuery

## Description

BigQuery data sources allow for the retrieval of historical feature values from BigQuery for building training datasets as well as materializing features into an online store.

* Either a table reference or a SQL query can be provided.
* No performance guarantees can be provided over SQL query-based sources. Please use table references where possible.

## Examples

Using a table reference

```python
from feast import BigQuerySource

my_bigquery_source = BigQuerySource(
    table_ref="gcp_project:bq_dataset.bq_table",
)
```

Using a query

```python
from feast import BigQuerySource

BigQuerySource(
    query="SELECT timestamp as ts, created, f1, f2 "
          "FROM `my_project.my_dataset.my_features`",
)
```

Configuration options are available [here](https://rtd.feast.dev/en/latest/index.html#feast.data_source.BigQuerySource).

