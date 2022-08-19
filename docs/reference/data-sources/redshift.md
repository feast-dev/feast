# Redshift source

## Description

Redshift data sources are Redshift tables or views.
These can be specified either by a table reference or a SQL query.
However, no performance guarantees can be provided for SQL query-based sources, so table references are recommended.

## Examples

Using a table name:

```python
from feast import RedshiftSource

my_redshift_source = RedshiftSource(
    table="redshift_table",
)
```

Using a query:

```python
from feast import RedshiftSource

my_redshift_source = RedshiftSource(
    query="SELECT timestamp as ts, created, f1, f2 "
          "FROM redshift_table",
)
```

The full set of configuration options is available [here](https://rtd.feast.dev/en/master/#feast.infra.offline_stores.redshift_source.RedshiftSource).

## Supported Types

Redshift data sources support all eight primitive types, but currently do not support array types.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).
