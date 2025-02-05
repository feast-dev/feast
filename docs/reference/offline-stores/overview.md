# Overview

## Functionality

Here are the methods exposed by the `OfflineStore` interface, along with the core functionality supported by the method:
* `get_historical_features`: point-in-time correct join to retrieve historical features
* `pull_latest_from_table_or_query`: retrieve latest feature values for materialization into the online store
* `pull_all_from_table_or_query`: retrieve a saved dataset
* `offline_write_batch`: persist dataframes to the offline store, primarily for push sources
* `write_logged_features`: persist logged features to the offline store, for feature logging

The first three of these methods all return a `RetrievalJob` specific to an offline store, such as a `SnowflakeRetrievalJob`. Here is a list of functionality supported by `RetrievalJob`s:
* export to dataframe
* export to arrow table
* export to arrow batches (to handle large datasets in memory)
* export to SQL
* export to data lake (S3, GCS, etc.)
* export to data warehouse
* export as Spark dataframe
* local execution of Python-based on-demand transforms
* remote execution of Python-based on-demand transforms
* persist results in the offline store
* preview the query plan before execution (`RetrievalJob`s are lazily executed)
* read partitioned data

## Functionality Matrix

There are currently four core offline store implementations: `DaskOfflineStore`, `BigQueryOfflineStore`, `SnowflakeOfflineStore`, and `RedshiftOfflineStore`.
There are several additional implementations contributed by the Feast community  (`PostgreSQLOfflineStore`, `SparkOfflineStore`, and `TrinoOfflineStore`), which are not guaranteed to be stable or to match the functionality of the core implementations.
Details for each specific offline store, such as how to configure it in a `feature_store.yaml`, can be found [here](README.md).

Below is a matrix indicating which offline stores support which methods.

| | Dask | BigQuery | Snowflake | Redshift | Postgres | Spark | Trino | Couchbase |
| :-------------------------------- | :-- | :-- | :-- | :-- | :-- | :-- | :-- | :-- |
| `get_historical_features`         | yes | yes | yes | yes | yes | yes | yes | yes |
| `pull_latest_from_table_or_query` | yes | yes | yes | yes | yes | yes | yes | yes |
| `pull_all_from_table_or_query`    | yes | yes | yes | yes | yes | yes | yes | yes |
| `offline_write_batch`             | yes | yes | yes | yes | no  | no  | no  | no  |
| `write_logged_features`           | yes | yes | yes | yes | no  | no  | no  | no  |


Below is a matrix indicating which `RetrievalJob`s support what functionality.

| | Dask | BigQuery | Snowflake | Redshift | Postgres | Spark | Trino | DuckDB | Couchbase |
| --------------------------------- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| export to dataframe                                   | yes | yes | yes | yes | yes | yes | yes | yes | yes | 
| export to arrow table                                 | yes | yes | yes | yes | yes | yes | yes | yes | yes |
| export to arrow batches                               | no  | no  | no  | yes | no  | no  | no  | no  | no  |
| export to SQL                                         | no  | yes | yes | yes | yes | no  | yes | no  | yes |
| export to data lake (S3, GCS, etc.)                   | no  | no  | yes | no  | yes | no  | no  | no  | yes |
| export to data warehouse                              | no  | yes | yes | yes | yes | no  | no  | no  | yes |
| export as Spark dataframe                             | no  | no  | yes | no  | no  | yes | no  | no  | no  |
| local execution of Python-based on-demand transforms  | yes | yes | yes | yes | yes | no  | yes | yes | yes |
| remote execution of Python-based on-demand transforms | no  | no  | no  | no  | no  | no  | no  | no  | no  |
| persist results in the offline store                  | yes | yes | yes | yes | yes | yes | no  | yes | yes |
| preview the query plan before execution               | yes | yes | yes | yes | yes | yes | yes | no  | yes |
| read partitioned data                                 | yes | yes | yes | yes | yes | yes | yes | yes | yes |
