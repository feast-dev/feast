# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [feast/core/CoreService.proto](#feast-core-coreservice-proto)
    - [ApplyFeatureSetRequest](#feast-core-applyfeaturesetrequest)
    - [ApplyFeatureSetResponse](#feast-core-applyfeaturesetresponse)
    - [GetFeastCoreVersionRequest](#feast-core-getfeastcoreversionrequest)
    - [GetFeastCoreVersionResponse](#feast-core-getfeastcoreversionresponse)
    - [GetFeatureSetsRequest](#feast-core-getfeaturesetsrequest)
    - [GetFeatureSetsRequest.Filter](#feast-core-getfeaturesetsrequest-filter)
    - [GetFeatureSetsResponse](#feast-core-getfeaturesetsresponse)
    - [GetStoresRequest](#feast-core-getstoresrequest)
    - [GetStoresRequest.Filter](#feast-core-getstoresrequest-filter)
    - [GetStoresResponse](#feast-core-getstoresresponse)
  
    - [ApplyFeatureSetResponse.Status](#feast-core-applyfeaturesetresponse-status)
  
  
    - [CoreService](#feast-core-coreservice)
  

- [feast/core/FeatureSet.proto](#feast-core-featureset-proto)
    - [EntitySpec](#feast-core-entityspec)
    - [FeatureSetSpec](#feast-core-featuresetspec)
    - [FeatureSpec](#feast-core-featurespec)
  
  
  
  

- [feast/core/Source.proto](#feast-core-source-proto)
    - [KafkaSourceConfig](#feast-core-kafkasourceconfig)
    - [Source](#feast-core-source)
  
    - [SourceType](#feast-core-sourcetype)
  
  
  

- [feast/core/Store.proto](#feast-core-store-proto)
    - [Store](#feast-core-store)
    - [Store.BigQueryConfig](#feast-core-store-bigqueryconfig)
    - [Store.CassandraConfig](#feast-core-store-cassandraconfig)
    - [Store.RedisConfig](#feast-core-store-redisconfig)
    - [Store.Subscription](#feast-core-store-subscription)
  
    - [Store.StoreType](#feast-core-store-storetype)
  
  
  

- [Scalar Value Types](#scalar-value-types)



<a name="feast/core/CoreService.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast/core/CoreService.proto



<a name="feast.core.ApplyFeatureSetRequest"></a>

### ApplyFeatureSetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_set | [FeatureSetSpec](#feast.core.FeatureSetSpec) |  | Feature set version and source will be ignored |






<a name="feast.core.ApplyFeatureSetResponse"></a>

### ApplyFeatureSetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_set | [FeatureSetSpec](#feast.core.FeatureSetSpec) |  | Feature set response has been enriched with version and source information |
| status | [ApplyFeatureSetResponse.Status](#feast.core.ApplyFeatureSetResponse.Status) |  |  |






<a name="feast.core.GetFeastCoreVersionRequest"></a>

### GetFeastCoreVersionRequest







<a name="feast.core.GetFeastCoreVersionResponse"></a>

### GetFeastCoreVersionResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [string](#string) |  |  |






<a name="feast.core.GetFeatureSetsRequest"></a>

### GetFeatureSetsRequest
Retrieves details for all versions of a specific feature set


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filter | [GetFeatureSetsRequest.Filter](#feast.core.GetFeatureSetsRequest.Filter) |  |  |






<a name="feast.core.GetFeatureSetsRequest.Filter"></a>

### GetFeatureSetsRequest.Filter



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_set_name | [string](#string) |  |  |
| feature_set_version | [string](#string) |  |  |






<a name="feast.core.GetFeatureSetsResponse"></a>

### GetFeatureSetsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_sets | [FeatureSetSpec](#feast.core.FeatureSetSpec) | repeated |  |






<a name="feast.core.GetStoresRequest"></a>

### GetStoresRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filter | [GetStoresRequest.Filter](#feast.core.GetStoresRequest.Filter) |  |  |






<a name="feast.core.GetStoresRequest.Filter"></a>

### GetStoresRequest.Filter



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |






<a name="feast.core.GetStoresResponse"></a>

### GetStoresResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store | [Store](#feast.core.Store) | repeated |  |





 <!-- end messages -->


<a name="feast.core.ApplyFeatureSetResponse.Status"></a>

### ApplyFeatureSetResponse.Status


| Name | Number | Description |
| ---- | ------ | ----------- |
| NO_CHANGE | 0 | Latest feature set version is consistent with provided feature set |
| CREATED | 1 | New feature set or feature set version created |
| ERROR | 2 | Error occurred while trying to apply changes |


 <!-- end enums -->

 <!-- end HasExtensions -->


<a name="feast.core.CoreService"></a>

### CoreService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetFeastCoreVersion | [GetFeastCoreVersionRequest](#feast.core.GetFeastCoreVersionRequest) | [GetFeastCoreVersionResponse](#feast.core.GetFeastCoreVersionResponse) | Retrieve version information about this Feast deployment |
| GetFeatureSets | [GetFeatureSetsRequest](#feast.core.GetFeatureSetsRequest) | [GetFeatureSetsResponse](#feast.core.GetFeatureSetsResponse) | Retrieve feature set details given a filter. Returns all featureSets matching that filter. |
| GetStores | [GetStoresRequest](#feast.core.GetStoresRequest) | [GetStoresResponse](#feast.core.GetStoresResponse) | Retrieve store details given a filter. Returns all stores matching that filter. |
| ApplyFeatureSet | [ApplyFeatureSetRequest](#feast.core.ApplyFeatureSetRequest) | [ApplyFeatureSetResponse](#feast.core.ApplyFeatureSetResponse) | Idempotent creation of feature set. Will not create a new feature set if schema does not change |

 <!-- end services -->



<a name="feast/core/FeatureSet.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast/core/FeatureSet.proto



<a name="feast.core.EntitySpec"></a>

### EntitySpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the entity. |
| value_type | [feast.types.ValueType.Enum](#feast.types.ValueType.Enum) |  | Value type of the feature. |






<a name="feast.core.FeatureSetSpec"></a>

### FeatureSetSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the featureSet. Must be unique. |
| version | [int32](#int32) |  | FeatureSet version. |
| entities | [EntitySpec](#feast.core.EntitySpec) | repeated | List of entities contained within this featureSet. This allows the feature to be used during joins between feature sets. If the featureSet is ingested into a store that supports keys, this value will be made a key. |
| features | [FeatureSpec](#feast.core.FeatureSpec) | repeated | List of features contained within this featureSet. |
| max_age | [google.protobuf.Duration](#google.protobuf.Duration) |  | Features in this feature set will only be retrieved if they are found after [time - max_age]. Missing or older feature values will be returned as nulls and indicated to end user |
| source | [Source](#feast.core.Source) |  | Source on which feature rows can be found |






<a name="feast.core.FeatureSpec"></a>

### FeatureSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the feature. |
| value_type | [feast.types.ValueType.Enum](#feast.types.ValueType.Enum) |  | Value type of the feature. |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->

 <!-- end services -->



<a name="feast/core/Source.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast/core/Source.proto



<a name="feast.core.KafkaSourceConfig"></a>

### KafkaSourceConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bootstrap_servers | [string](#string) |  | - bootstrapServers: [comma delimited value of host[:port]] |
| topic | [string](#string) |  | - topics: [Kafka topic name. This value is provisioned by core and should not be set by the user.] |






<a name="feast.core.Source"></a>

### Source



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [SourceType](#feast.core.SourceType) |  | The kind of data source Feast should connect to in order to retrieve FeatureRow value |
| kafka_source_config | [KafkaSourceConfig](#feast.core.KafkaSourceConfig) |  |  |





 <!-- end messages -->


<a name="feast.core.SourceType"></a>

### SourceType


| Name | Number | Description |
| ---- | ------ | ----------- |
| INVALID | 0 |  |
| KAFKA | 1 |  |


 <!-- end enums -->

 <!-- end HasExtensions -->

 <!-- end services -->



<a name="feast/core/Store.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast/core/Store.proto



<a name="feast.core.Store"></a>

### Store
Store provides a location where Feast reads and writes feature values.
Feature values will be written to the Store in the form of FeatureRow elements.
The way FeatureRow is encoded and decoded when it is written to and read from
the Store depends on the type of the Store.

For example, a FeatureRow will materialize as a row in a table in 
BigQuery but it will materialize as a key, value pair element in Redis.


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the store. |
| type | [Store.StoreType](#feast.core.Store.StoreType) |  | Type of store. |
| subscriptions | [Store.Subscription](#feast.core.Store.Subscription) | repeated | Feature sets to subscribe to. |
| redis_config | [Store.RedisConfig](#feast.core.Store.RedisConfig) |  |  |
| bigquery_config | [Store.BigQueryConfig](#feast.core.Store.BigQueryConfig) |  |  |
| cassandra_config | [Store.CassandraConfig](#feast.core.Store.CassandraConfig) |  |  |






<a name="feast.core.Store.BigQueryConfig"></a>

### Store.BigQueryConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| project_id | [string](#string) |  |  |
| dataset_id | [string](#string) |  |  |






<a name="feast.core.Store.CassandraConfig"></a>

### Store.CassandraConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host | [string](#string) |  |  |
| port | [int32](#int32) |  |  |






<a name="feast.core.Store.RedisConfig"></a>

### Store.RedisConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host | [string](#string) |  |  |
| port | [int32](#int32) |  |  |






<a name="feast.core.Store.Subscription"></a>

### Store.Subscription



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of featureSet to subscribe to. |
| version | [string](#string) |  | Versions of the given featureSet that will be ingested into this store. Valid options for version: latest: only subscribe to latest version of feature set [version number]: pin to a specific version >[version number]: subscribe to all versions larger than or equal to [version number] |





 <!-- end messages -->


<a name="feast.core.Store.StoreType"></a>

### Store.StoreType


| Name | Number | Description |
| ---- | ------ | ----------- |
| INVALID | 0 |  |
| REDIS | 1 | Redis stores a FeatureRow element as a key, value pair.

The Redis data types used (https://redis.io/topics/data-types): - key: STRING - value: STRING

Encodings: - key: byte array of RedisKey (refer to feast.storage.RedisKey) - value: byte array of FeatureRow (refer to feast.types.FeatureRow) |
| BIGQUERY | 2 | BigQuery stores a FeatureRow element as a row in a BigQuery table.

Table name is derived from the feature set name and version as: [feature_set_name]_v[feature_set_version] 

For example: A feature row for feature set "driver" and version "1" will be written to table "driver_v1".

The entities and features in a FeatureSetSpec corresponds to the fields in the BigQuery table (these make up the BigQuery schema). The name of the entity spec and feature spec corresponds to the column names, and the value_type of entity spec and feature spec corresponds to BigQuery standard SQL data type of the column. 

The following BigQuery fields are reserved for Feast internal use. Ingestion of entity or feature spec with names identical to the following field names will raise an exception during ingestion.

 column_name | column_data_type | description ====================|==================|================================ - event_timestamp | TIMESTAMP | event time of the FeatureRow - created_timestamp | TIMESTAMP | processing time of the ingestion of the FeatureRow - job_id | STRING | identifier for the job that writes the FeatureRow to the corresponding BigQuery table

BigQuery table created will be partitioned by the field "event_timestamp" of the FeatureRow (https://cloud.google.com/bigquery/docs/partitioned-tables).

Since newer version of feature set can introduce breaking, non backward- compatible BigQuery schema updates, incrementing the version of a feature set will result in the creation of a new empty BigQuery table with the new schema.

The following table shows how ValueType in Feast is mapped to BigQuery Standard SQL data types (https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types):

BYTES : BYTES STRING : STRING INT32 : INT64 INT64 : IN64 DOUBLE : FLOAT64 FLOAT : FLOAT64 BOOL : BOOL BYTES_LIST : ARRAY STRING_LIST : ARRAY INT32_LIST : ARRAY INT64_LIST : ARRAY DOUBLE_LIST : ARRAY FLOAT_LIST : ARRAY BOOL_LIST : ARRAY

The column mode in BigQuery is set to "Nullable" such that unset Value in a FeatureRow corresponds to NULL value in BigQuery. |
| CASSANDRA | 3 | Unsupported in Feast 0.3 |


 <!-- end enums -->

 <!-- end HasExtensions -->

 <!-- end services -->



## Scalar Value Types

| .proto Type | Notes | C++ Type | Java Type | Python Type |
| ----------- | ----- | -------- | --------- | ----------- |
| <a name="double" /> double |  | double | double | float |
| <a name="float" /> float |  | float | float | float |
| <a name="int32" /> int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int |
| <a name="int64" /> int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long |
| <a name="uint32" /> uint32 | Uses variable-length encoding. | uint32 | int | int/long |
| <a name="uint64" /> uint64 | Uses variable-length encoding. | uint64 | long | int/long |
| <a name="sint32" /> sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int |
| <a name="sint64" /> sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long |
| <a name="fixed32" /> fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int |
| <a name="fixed64" /> fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long |
| <a name="sfixed32" /> sfixed32 | Always four bytes. | int32 | int | int |
| <a name="sfixed64" /> sfixed64 | Always eight bytes. | int64 | long | int/long |
| <a name="bool" /> bool |  | bool | boolean | boolean |
| <a name="string" /> string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode |
| <a name="bytes" /> bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str |
