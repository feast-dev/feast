# Redshift

## Description

Redshift data sources allow for the retrieval of historical feature values from Redshift for building training datasets as well as materializing features into an online store.

* Either a table name or a SQL query can be provided.
* No performance guarantees can be provided over SQL query-based sources. Please use table references where possible.

## Examples

Using a table name

```python
from feast import RedshiftSource

my_redshift_source = RedshiftSource(
    table="redshift_table",
)
```

Using a query

```python
from feast import RedshiftSource

my_redshift_source = RedshiftSource(
    query="SELECT timestamp as ts, created, f1, f2 "
          "FROM redshift_table",
)
```

Configuration options are available [here](https://rtd.feast.dev/en/master/feast.html?#feast.RedshiftSource).

