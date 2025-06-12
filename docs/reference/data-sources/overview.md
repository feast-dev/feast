# Overview

## Functionality

In Feast, each batch data source is associated with corresponding offline stores.
For example, a `SnowflakeSource` can only be processed by the Snowflake offline store, while a `FileSource` can be processed by both File and DuckDB offline stores.
Otherwise, the primary difference between batch data sources is the set of supported types.
Feast has an internal type system, and aims to support eight primitive types (`bytes`, `string`, `int32`, `int64`, `float32`, `float64`, `bool`, and `timestamp`) along with the corresponding array types.
However, not every batch data source supports all of these types.

For more details on the Feast type system, see [here](../type-system.md).

## Functionality Matrix

There are currently four core batch data source implementations: `FileSource`, `BigQuerySource`, `SnowflakeSource`, and `RedshiftSource`.
There are several additional implementations contributed by the Feast community (`PostgreSQLSource`, `SparkSource`, and `TrinoSource`), which are not guaranteed to be stable or to match the functionality of the core implementations.
Details for each specific data source can be found [here](README.md).

Below is a matrix indicating which data sources support which types.

| | File | BigQuery | Snowflake | Redshift | Postgres | Spark | Trino | Couchbase |
| :-------------------------------- | :-- | :-- |:----------| :-- | :-- | :-- | :-- |:----------|
| `bytes`     | yes | yes | yes       | yes | yes | yes | yes | yes |
| `string`    | yes | yes | yes       | yes | yes | yes | yes | yes |
| `int32`     | yes | yes | yes       | yes | yes | yes | yes | yes |
| `int64`     | yes | yes | yes       | yes | yes | yes | yes | yes |
| `float32`   | yes | yes | yes       | yes | yes | yes | yes | yes |
| `float64`   | yes | yes | yes       | yes | yes | yes | yes | yes |
| `bool`      | yes | yes | yes       | yes | yes | yes | yes | yes |
| `timestamp` | yes | yes | yes       | yes | yes | yes | yes | yes |
| array types | yes | yes | yes       | no  | yes | yes | yes | no  |
