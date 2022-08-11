# Overview

## Functionality

In Feast, each batch data source is associated with a corresponding offline store.
For example, a `SnowflakeSource` can only be processed by the Snowflake offline store.
Otherwise, the primary difference between batch data sources is the set of supported types.
Feast has an internal type system, and aims to support eight primitive types (`bytes`, `string`, `int32`, `int64`, `float32`, `float64`, `bool`, and `timestamp`) along with the corresponding array types.
However, not every batch data source supports all of these types.

The logic to map types from data sources to Feast's internal type system is contained in [`type_map.py`](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/type_map.py), which is the ultimate source of truth on whether a specific data source supports a specific type.

## Functionality Matrix

There are currently four core batch data source implementations: `FileSource`, `BigQuerySource`, `SnowflakeSource`, and `RedshiftSource`.
There are several additional implementations contributed by the Feast community (`PostgreSQLSource`, `SparkSource`, and `TrinoSource`), which are not guaranteed to be stable or to match the functionality of the core implementations.
Details for each specific data source can be found [here](README.md).

Below is a matrix indicating which data sources support which types.

| | File | BigQuery | Snowflake | Redshift | Postgres | Spark | Trino |
| :-------------------------------- | :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| `bytes`     | yes | yes | yes | yes | yes | yes | yes |
| `string`    | yes | yes | yes | yes | yes | yes | yes |
| `int32`     | yes | yes | yes | yes | yes | yes | yes |
| `int64`     | yes | yes | yes | yes | yes | yes | yes |
| `float32`   | yes | yes | yes | yes | yes | yes | yes |
| `float64`   | yes | yes | yes | yes | yes | yes | yes |
| `bool`      | yes | yes | yes | yes | yes | yes | yes |
| `timestamp` | yes | yes | yes | yes | yes | yes | yes |
| array types | yes | yes | no  | no  | yes | yes | no  |