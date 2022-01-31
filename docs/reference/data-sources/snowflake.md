# Snowflake

## Description

Snowflake data sources allow for the retrieval of historical feature values from Snowflake for building training datasets as well as materializing features into an online store.

* Either a table reference or a SQL query can be provided.

## Examples

Using a table reference

```python
from feast import SnowflakeSource

my_snowflake_source = SnowflakeSource(
    database="FEAST",
    schema="PUBLIC",
    table="FEATURE_TABLE",
)
```

Using a query

```python
from feast import SnowflakeSource

my_snowflake_source = SnowflakeSource(
    query="""
    SELECT
        timestamp_column AS "ts",
        "created",
        "f1",
        "f2"
    FROM
        `FEAST.PUBLIC.FEATURE_TABLE`
      """,
)
```

One thing to remember is how Snowflake handles table and column name conventions.
You can read more about quote identifiers [here](https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html)

Configuration options are available [here](https://rtd.feast.dev/en/latest/index.html#feast.data_source.SnowflakeSource).
