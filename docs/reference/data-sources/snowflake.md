# Snowflake source

## Description

Snowflake data sources are Snowflake tables or views.
These can be specified either by a table reference or a SQL query.

## Examples

Using a table reference:

```python
from feast import SnowflakeSource

my_snowflake_source = SnowflakeSource(
    database="FEAST",
    schema="PUBLIC",
    table="FEATURE_TABLE",
)
```

Using a query:

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

{% hint style="warning" %}
Be careful about how Snowflake handles table and column name conventions.
In particular, you can read more about quote identifiers [here](https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html).
{% endhint %}

The full set of configuration options is available [here](https://rtd.feast.dev/en/latest/index.html#feast.infra.offline_stores.snowflake_source.SnowflakeSource).

## Supported Types

Snowflake data sources support all eight primitive types, but currently do not support array types.
For a comparison against other batch data sources, please see [here](overview.md#functionality-matrix).
