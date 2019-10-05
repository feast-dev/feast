# Protocol Documentation

## Table of Contents

* [feast/core/CoreService.proto](core.md#feast/core/CoreService.proto)
  * [ApplyFeatureSetRequest](core.md#feast.core.ApplyFeatureSetRequest)
  * [ApplyFeatureSetResponse](core.md#feast.core.ApplyFeatureSetResponse)
  * [GetFeastCoreVersionRequest](core.md#feast.core.GetFeastCoreVersionRequest)
  * [GetFeastCoreVersionResponse](core.md#feast.core.GetFeastCoreVersionResponse)
  * [GetFeatureSetsRequest](core.md#feast.core.GetFeatureSetsRequest)
  * [GetFeatureSetsRequest.Filter](core.md#feast.core.GetFeatureSetsRequest.Filter)
  * [GetFeatureSetsResponse](core.md#feast.core.GetFeatureSetsResponse)
  * [GetStoresRequest](core.md#feast.core.GetStoresRequest)
  * [GetStoresRequest.Filter](core.md#feast.core.GetStoresRequest.Filter)
  * [GetStoresResponse](core.md#feast.core.GetStoresResponse)
  * [ApplyFeatureSetResponse.Status](core.md#feast.core.ApplyFeatureSetResponse.Status)
* [CoreService](core.md#feast.core.CoreService)
* [feast/core/FeatureSet.proto](core.md#feast/core/FeatureSet.proto)
  * [EntitySpec](core.md#feast.core.EntitySpec)
  * [FeatureSetSpec](core.md#feast.core.FeatureSetSpec)
  * [FeatureSpec](core.md#feast.core.FeatureSpec)
* [feast/core/Source.proto](core.md#feast/core/Source.proto)
  * [KafkaSourceConfig](core.md#feast.core.KafkaSourceConfig)
  * [Source](core.md#feast.core.Source)
  * [SourceType](core.md#feast.core.SourceType)
* [feast/core/Store.proto](core.md#feast/core/Store.proto)
  * [Store](core.md#feast.core.Store)
  * [Store.BigQueryConfig](core.md#feast.core.Store.BigQueryConfig)
  * [Store.CassandraConfig](core.md#feast.core.Store.CassandraConfig)
  * [Store.RedisConfig](core.md#feast.core.Store.RedisConfig)
  * [Store.Subscription](core.md#feast.core.Store.Subscription)
  * [Store.StoreType](core.md#feast.core.Store.StoreType)
* [Scalar Value Types](core.md#scalar-value-types)

[Top](core.md#top)

## feast/core/CoreService.proto

### ApplyFeatureSetRequest

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| feature\_set | [FeatureSetSpec](core.md#feast.core.FeatureSetSpec) |  | Feature set version and source will be ignored |

### ApplyFeatureSetResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| feature\_set | [FeatureSetSpec](core.md#feast.core.FeatureSetSpec) |  | Feature set response has been enriched with version and source information |
| status | [ApplyFeatureSetResponse.Status](core.md#feast.core.ApplyFeatureSetResponse.Status) |  |  |

### GetFeastCoreVersionRequest

### GetFeastCoreVersionResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| version | [string](core.md#string) |  |  |

### GetFeatureSetsRequest

Retrieves details for all versions of a specific feature set

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| filter | [GetFeatureSetsRequest.Filter](core.md#feast.core.GetFeatureSetsRequest.Filter) |  |  |

### GetFeatureSetsRequest.Filter

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| feature\_set\_name | [string](core.md#string) |  |  |
| feature\_set\_version | [string](core.md#string) |  |  |

### GetFeatureSetsResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| feature\_sets | [FeatureSetSpec](core.md#feast.core.FeatureSetSpec) | repeated |  |

### GetStoresRequest

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| filter | [GetStoresRequest.Filter](core.md#feast.core.GetStoresRequest.Filter) |  |  |

### GetStoresRequest.Filter

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](core.md#string) |  |  |

### GetStoresResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| store | [Store](core.md#feast.core.Store) | repeated |  |

### ApplyFeatureSetResponse.Status

| Name | Number | Description |
| :--- | :--- | :--- |
| NO\_CHANGE | 0 | Latest feature set version is consistent with provided feature set |
| CREATED | 1 | New feature set or feature set version created |
| ERROR | 2 | Error occurred while trying to apply changes |

### CoreService

| Method Name | Request Type | Response Type | Description |
| :--- | :--- | :--- | :--- |
| GetFeastCoreVersion | [GetFeastCoreVersionRequest](core.md#feast.core.GetFeastCoreVersionRequest) | [GetFeastCoreVersionResponse](core.md#feast.core.GetFeastCoreVersionResponse) | Retrieve version information about this Feast deployment |
| GetFeatureSets | [GetFeatureSetsRequest](core.md#feast.core.GetFeatureSetsRequest) | [GetFeatureSetsResponse](core.md#feast.core.GetFeatureSetsResponse) | Retrieve feature set details given a filter. Returns all featureSets matching that filter. |
| GetStores | [GetStoresRequest](core.md#feast.core.GetStoresRequest) | [GetStoresResponse](core.md#feast.core.GetStoresResponse) | Retrieve store details given a filter. Returns all stores matching that filter. |
| ApplyFeatureSet | [ApplyFeatureSetRequest](core.md#feast.core.ApplyFeatureSetRequest) | [ApplyFeatureSetResponse](core.md#feast.core.ApplyFeatureSetResponse) | Idempotent creation of feature set. Will not create a new feature set if schema does not change |

[Top](core.md#top)

## feast/core/FeatureSet.proto

### EntitySpec

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](core.md#string) |  | Name of the entity. |
| value\_type | [feast.types.ValueType.Enum](core.md#feast.types.ValueType.Enum) |  | Value type of the feature. |

### FeatureSetSpec

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](core.md#string) |  | Name of the featureSet. Must be unique. |
| version | [int32](core.md#int32) |  | FeatureSet version. |
| entities | [EntitySpec](core.md#feast.core.EntitySpec) | repeated | List of entities contained within this featureSet. This allows the feature to be used during joins between feature sets. If the featureSet is ingested into a store that supports keys, this value will be made a key. |
| features | [FeatureSpec](core.md#feast.core.FeatureSpec) | repeated | List of features contained within this featureSet. |
| max\_age | [google.protobuf.Duration](core.md#google.protobuf.Duration) |  | Features in this feature set will only be retrieved if they are found after \[time - max\_age\]. Missing or older feature values will be returned as nulls and indicated to end user |
| source | [Source](core.md#feast.core.Source) |  | Source on which feature rows can be found |

### FeatureSpec

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](core.md#string) |  | Name of the feature. |
| value\_type | [feast.types.ValueType.Enum](core.md#feast.types.ValueType.Enum) |  | Value type of the feature. |

[Top](core.md#top)

## feast/core/Source.proto

### KafkaSourceConfig

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| bootstrap\_servers | [string](core.md#string) |  | - bootstrapServers: \[comma delimited value of host\[:port\]\] |
| topic | [string](core.md#string) |  | - topics: \[Kafka topic name. This value is provisioned by core and should not be set by the user.\] |

### Source

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| type | [SourceType](core.md#feast.core.SourceType) |  | The kind of data source Feast should connect to in order to retrieve FeatureRow value |
| kafka\_source\_config | [KafkaSourceConfig](core.md#feast.core.KafkaSourceConfig) |  |  |

### SourceType

| Name | Number | Description |
| :--- | :--- | :--- |
| INVALID | 0 |  |
| KAFKA | 1 |  |

[Top](core.md#top)

## feast/core/Store.proto

### Store

Store provides a location where Feast reads and writes feature values. Feature values will be written to the Store in the form of FeatureRow elements. The way FeatureRow is encoded and decoded when it is written to and read from the Store depends on the type of the Store.

For example, a FeatureRow will materialize as a row in a table in BigQuery but it will materialize as a key, value pair element in Redis.

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](core.md#string) |  | Name of the store. |
| type | [Store.StoreType](core.md#feast.core.Store.StoreType) |  | Type of store. |
| subscriptions | [Store.Subscription](core.md#feast.core.Store.Subscription) | repeated | Feature sets to subscribe to. |
| redis\_config | [Store.RedisConfig](core.md#feast.core.Store.RedisConfig) |  |  |
| bigquery\_config | [Store.BigQueryConfig](core.md#feast.core.Store.BigQueryConfig) |  |  |
| cassandra\_config | [Store.CassandraConfig](core.md#feast.core.Store.CassandraConfig) |  |  |

### Store.BigQueryConfig

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| project\_id | [string](core.md#string) |  |  |
| dataset\_id | [string](core.md#string) |  |  |

### Store.CassandraConfig

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| host | [string](core.md#string) |  |  |
| port | [int32](core.md#int32) |  |  |

### Store.RedisConfig

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| host | [string](core.md#string) |  |  |
| port | [int32](core.md#int32) |  |  |

### Store.Subscription

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](core.md#string) |  | Name of featureSet to subscribe to. |
| version | [string](core.md#string) |  | Versions of the given featureSet that will be ingested into this store. Valid options for version: latest: only subscribe to latest version of feature set \[version number\]: pin to a specific version &gt;\[version number\]: subscribe to all versions larger than or equal to \[version number\] |

### Store.StoreType

| Name | Number | Description |
| :--- | :--- | :--- |
| INVALID | 0 |  |
| REDIS | 1 | Redis stores a FeatureRow element as a key, value pair. |

The Redis data types used \([https://redis.io/topics/data-types](https://redis.io/topics/data-types)\): - key: STRING - value: STRING

Encodings: - key: byte array of RedisKey \(refer to feast.storage.RedisKey\) - value: byte array of FeatureRow \(refer to feast.types.FeatureRow\) \| \| BIGQUERY \| 2 \| BigQuery stores a FeatureRow element as a row in a BigQuery table.

Table name is derived from the feature set name and version as: \[feature\_set\_name\]\_v\[feature\_set\_version\]

For example: A feature row for feature set "driver" and version "1" will be written to table "driver\_v1".

The entities and features in a FeatureSetSpec corresponds to the fields in the BigQuery table \(these make up the BigQuery schema\). The name of the entity spec and feature spec corresponds to the column names, and the value\_type of entity spec and feature spec corresponds to BigQuery standard SQL data type of the column.

The following BigQuery fields are reserved for Feast internal use. Ingestion of entity or feature spec with names identical to the following field names will raise an exception during ingestion.

column\_name \| column\_data\_type \| description ====================\|==================\|================================ - event\_timestamp \| TIMESTAMP \| event time of the FeatureRow - created\_timestamp \| TIMESTAMP \| processing time of the ingestion of the FeatureRow - job\_id \| STRING \| identifier for the job that writes the FeatureRow to the corresponding BigQuery table

BigQuery table created will be partitioned by the field "event\_timestamp" of the FeatureRow \([https://cloud.google.com/bigquery/docs/partitioned-tables](https://cloud.google.com/bigquery/docs/partitioned-tables)\).

Since newer version of feature set can introduce breaking, non backward- compatible BigQuery schema updates, incrementing the version of a feature set will result in the creation of a new empty BigQuery table with the new schema.

The following table shows how ValueType in Feast is mapped to BigQuery Standard SQL data types \([https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)\):

BYTES : BYTES STRING : STRING INT32 : INT64 INT64 : IN64 DOUBLE : FLOAT64 FLOAT : FLOAT64 BOOL : BOOL BYTES\_LIST : ARRAY STRING\_LIST : ARRAY INT32\_LIST : ARRAY INT64\_LIST : ARRAY DOUBLE\_LIST : ARRAY FLOAT\_LIST : ARRAY BOOL\_LIST : ARRAY

The column mode in BigQuery is set to "Nullable" such that unset Value in a FeatureRow corresponds to NULL value in BigQuery. \| \| CASSANDRA \| 3 \| Unsupported in Feast 0.3 \|

## Scalar Value Types

| .proto Type | Notes | C++ Type | Java Type | Python Type |
| :--- | :--- | :--- | :--- | :--- |
|  double |  | double | double | float |
|  float |  | float | float | float |
|  int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int |
|  int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long |
|  uint32 | Uses variable-length encoding. | uint32 | int | int/long |
|  uint64 | Uses variable-length encoding. | uint64 | long | int/long |
|  sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int |
|  sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long |
|  fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int |
|  fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long |
|  sfixed32 | Always four bytes. | int32 | int | int |
|  sfixed64 | Always eight bytes. | int64 | long | int/long |
|  bool |  | bool | boolean | boolean |
|  string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode |
|  bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str |

