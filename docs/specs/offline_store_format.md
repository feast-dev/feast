# Feast Offline Store Format

## Overview
This document describes feature data storage format for offline retrieval in Feast.

One of the design goals of Feast is being able to plug seamlessly into existing infrastructure, and avoiding adding operational overhead to your ML stack. So instead of being yet another database, Feast relies on existing data storage facilities to store offline feature data.

Feast provides first class support for the following data warehouses (DWH) to store feature data offline out of the box:
* [BigQuery](https://cloud.google.com/bigquery)
* [Snowflake](https://www.snowflake.com/)
* [Redshift](https://aws.amazon.com/redshift/)

The integration between Feast and the DWH is highly configurable, but at the same time there are some non-configurable implications and assumptions that Feast imposes on table schemas and mapping between database-native types and Feast type system. This is what this document is about.

## Terminology
For brevity, below we'll use just "DWH" for the data warehouse that is used as an offline storage engine for feature data.

For common Feast terms, like "Feature Table", "Entity" please refer to [Feast glossary](https://github.com/feast-dev/feast/blob/master/docs/concepts/glossary.md).

## Table schema
Feature data is stored in tables in the DWH. There is one DWH table per Feast Feature Table. Each table in DWH is expected to have three groups of columns:
* One or more Entity columns. Together they compose an [Entity Key](https://github.com/feast-dev/feast/blob/master/docs/concepts/glossary.md#entity-key). Their types should match Entity type definitions in Feast metadata, according to the mapping for the specific DWH engine being used. The name of the column must match the entity name.
* One [Entity timestamp](https://github.com/feast-dev/feast/blob/master/docs/concepts/glossary.md#entity-timestamp) column, also called "event timestamp". The type is DWH-specific timestamp type. The name of the column is set when you configure the offline data source.
* Optional "created timestamp" column. This is typically wallclock time of when the feature value was computed. If there are two feature values with the same Entity Key and Event Timestamp, the one with more recent Created Timestamp will take precedence. The type is DWH-specific timestamp type. The name of the column is set when you configure the offline data source.
* One or more feature value columns. Their types should match Feature type defined in Feast metadata, according to the mapping for the specific DWH engine being used. The names must match feature names, but can optionally be remapped when configuring the offline data source.


## Type mappings

#### Pandas types
Here's how Feast types map to Pandas types for Feast APIs that take in or return a Pandas dataframe:

| Feast Type | Pandas Type |
|-------------|--|
| Event Timestamp |   `datetime64[ns]` |
| BYTES | `bytes` |
| STRING | `str` , `category`|
| INT32 | `int16`, `uint16`, `int32`, `uint32` |
| INT64 | `int64`, `uint64` |
| UNIX_TIMESTAMP | `datetime64[ns]`, `datetime64[ns, tz]` |
| DOUBLE | `float64` |
| FLOAT | `float32` |
| BOOL | `bool`|
| BYTES\_LIST | `list[bytes]` |
| STRING\_LIST | `list[str]`|
| INT32\_LIST | `list[int]`|
| INT64\_LIST | `list[int]`|
| UNIX_TIMESTAMP\_LIST | `list[unix_timestamp]`|
| DOUBLE\_LIST | `list[float]`|
| FLOAT\_LIST | `list[float]`|
| BOOL\_LIST | `list[bool]`|

Note that this mapping is non-injective, that is more than one Pandas type may corresponds to one Feast type (but not vice versa). In these cases, when converting Feast values to Pandas, the **first** Pandas type in the table above is used.

Feast array types are mapped to a pandas column with object dtype, that contains a Python array of corresponding type.

Another thing to note is Feast doesn't support timestamp type for entity and feature columns. Values of datetime type in pandas dataframe are converted to int64 if they are found in entity and feature columns. In order to easily differentiate int64 to timestamp features, there is a UNIX_TIMESTAMP type that is an int64 under the hood.  

#### BigQuery types
Here's how Feast types map to BigQuery types when using BigQuery for offline storage when reading data from BigQuery to the online store:

| Feast Type | BigQuery Type |
|-------------|--|
| Event Timestamp |   `DATETIME` |
| BYTES | `BYTES` |
| STRING | `STRING` |
| INT32 | `INT64 / INTEGER` |
| INT64 | `INT64 / INTEGER` |
| UNIX_TIMESTAMP | `INT64 / INTEGER` |
| DOUBLE | `FLOAT64 / FLOAT` |
| FLOAT | `FLOAT64 / FLOAT` |
| BOOL | `BOOL`|
| BYTES\_LIST | `ARRAY<BYTES>` |
| STRING\_LIST | `ARRAY<STRING>`|
| INT32\_LIST | `ARRAY<INT64>`|
| INT64\_LIST | `ARRAY<INT64>`|
| UNIX_TIMESTAMP\_LIST | `ARRAY<INT64>`|
| DOUBLE\_LIST | `ARRAY<FLOAT64>`|
| FLOAT\_LIST | `ARRAY<FLOAT64>`|
| BOOL\_LIST | `ARRAY<BOOL>`|

Values that are not specified by the table above will cause an error on conversion.

#### Snowflake Types
Here's how Feast types map to Snowflake types when using Snowflake for offline storage
See source here:
https://docs.snowflake.com/en/user-guide/python-connector-pandas.html#snowflake-to-pandas-data-mapping

| Feast Type | Snowflake Python Type |
|-------------|--|
| Event Timestamp |   `DATETIME64[NS]` |
| UNIX_TIMESTAMP | `DATETIME64[NS]` |
| STRING | `STR` |
| INT32 | `INT8 / UINT8 / INT16 / UINT16 / INT32 / UINT32` |
| INT64 | `INT64 / UINT64` |
| DOUBLE | `FLOAT64` |
