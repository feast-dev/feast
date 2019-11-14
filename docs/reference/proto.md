# Core API

## Table of Contents

* [feast.core.CoreService.proto](proto.md#feast-core-coreservice-proto)
  * [CoreService](proto.md#coreservice)
  * [ApplyFeatureSetRequest](proto.md#applyfeaturesetrequest)
  * [ApplyFeatureSetResponse](proto.md#applyfeaturesetresponse)
  * [GetFeastCoreVersionRequest](proto.md#getfeastcoreversionrequest)
  * [GetFeastCoreVersionResponse](proto.md#getfeastcoreversionresponse)
  * [GetFeatureSetsRequest](proto.md#getfeaturesetsrequest)
  * [GetFeatureSetsRequest.Filter](proto.md#filter)
  * [GetFeatureSetsResponse](proto.md#getfeaturesetsresponse)
  * [GetStoresRequest](proto.md#getstoresrequest)
  * [GetStoresRequest.Filter](proto.md#filter)
  * [GetStoresResponse](proto.md#getstoresresponse)
  * [UpdateStoreRequest](proto.md#updatestorerequest)
  * [UpdateStoreResponse](proto.md#updatestoreresponse)
  * [ApplyFeatureSetResponse.Status](proto.md#status)
  * [UpdateStoreResponse.Status](proto.md#status)
* [feast.core.FeatureSet.proto](proto.md#feast-core-featureset-proto)
  * [EntitySpec](proto.md#entityspec)
  * [FeatureSetSpec](proto.md#featuresetspec)
  * [FeatureSpec](proto.md#featurespec)
* [feast.core.Source.proto](proto.md#feast-core-source-proto)
  * [KafkaSourceConfig](proto.md#kafkasourceconfig)
  * [Source](proto.md#source)
  * [SourceType](proto.md#sourcetype)
* [feast.core.Store.proto](proto.md#feast-core-store-proto)
  * [Store](proto.md#store)
  * [Store.BigQueryConfig](proto.md#bigqueryconfig)
  * [Store.CassandraConfig](proto.md#cassandraconfig)
  * [Store.RedisConfig](proto.md#redisconfig)
  * [Store.Subscription](proto.md#subscription)
  * [Store.StoreType](proto.md#storetype)
* [feast.serving.ServingService.proto](proto.md#feast-serving-servingservice-proto)
  * [ServingService](proto.md#servingservice)
  * [DatasetSource](proto.md#datasetsource)
  * [DatasetSource.FileSource](proto.md#filesource)
  * [FeatureSet](proto.md#featureset)
  * [GetBatchFeaturesRequest](proto.md#getbatchfeaturesrequest)
  * [GetBatchFeaturesResponse](proto.md#getbatchfeaturesresponse)
  * [GetFeastServingInfoRequest](proto.md#getfeastservinginforequest)
  * [GetFeastServingInfoResponse](proto.md#getfeastservinginforesponse)
  * [GetJobRequest](proto.md#getjobrequest)
  * [GetJobResponse](proto.md#getjobresponse)
  * [GetOnlineFeaturesRequest](proto.md#getonlinefeaturesrequest)
  * [GetOnlineFeaturesRequest.EntityRow](proto.md#entityrow)
  * [GetOnlineFeaturesRequest.EntityRow.FieldsEntry](proto.md#fieldsentry)
  * [GetOnlineFeaturesResponse](proto.md#getonlinefeaturesresponse)
  * [GetOnlineFeaturesResponse.FieldValues](proto.md#fieldvalues)
  * [GetOnlineFeaturesResponse.FieldValues.FieldsEntry](proto.md#fieldsentry)
  * [Job](proto.md#job)
  * [DataFormat](proto.md#dataformat)
  * [FeastServingType](proto.md#feastservingtype)
  * [JobStatus](proto.md#jobstatus)
  * [JobType](proto.md#jobtype)
* [feast.storage.Redis.proto](proto.md#feast-storage-redis-proto)
  * [RedisKey](proto.md#rediskey)
* [feast.types.FeatureRow.proto](proto.md#feast-types-featurerow-proto)
  * [FeatureRow](proto.md#featurerow)
* [feast.types.FeatureRowExtended.proto](proto.md#feast-types-featurerowextended-proto)
  * [Attempt](proto.md#attempt)
  * [Error](proto.md#error)
  * [FeatureRowExtended](proto.md#featurerowextended)
* [feast.types.Field.proto](proto.md#feast-types-field-proto)
  * [Field](proto.md#field)
* [feast.types.Value.proto](proto.md#feast-types-value-proto)
  * [BoolList](proto.md#boollist)
  * [BytesList](proto.md#byteslist)
  * [DoubleList](proto.md#doublelist)
  * [FloatList](proto.md#floatlist)
  * [Int32List](proto.md#int32list)
  * [Int64List](proto.md#int64list)
  * [StringList](proto.md#stringlist)
  * [Value](proto.md#value)
  * [ValueType](proto.md#valuetype)
  * [ValueType.Enum](proto.md#enum)
* [Scalar Value Types](proto.md#scalar-value-types)

[Top](proto.md#top)

## feast.core.CoreService.proto

### CoreService

| Method Name | Request Type | Response Type | Description |
| :--- | :--- | :--- | :--- |
| GetFeastCoreVersion | [GetFeastCoreVersionRequest](proto.md#GetFeastCoreVersionRequest) | [GetFeastCoreVersionResponse](proto.md#GetFeastCoreVersionResponse) | Retrieve version information about this Feast deployment |
| GetFeatureSets | [GetFeatureSetsRequest](proto.md#GetFeatureSetsRequest) | [GetFeatureSetsResponse](proto.md#GetFeatureSetsResponse) | Retrieve feature set details given a filter. |

Returns all feature sets matching that filter. If none are found, an empty list will be returned. If no filter is provided in the request, the response will contain all the feature sets currently stored in the registry. \| \| GetStores \| [GetStoresRequest](proto.md#GetStoresRequest) \| [GetStoresResponse](proto.md#GetStoresResponse) \| Retrieve store details given a filter.

Returns all stores matching that filter. If none are found, an empty list will be returned. If no filter is provided in the request, the response will contain all the stores currently stored in the registry. \| \| ApplyFeatureSet \| [ApplyFeatureSetRequest](proto.md#ApplyFeatureSetRequest) \| [ApplyFeatureSetResponse](proto.md#ApplyFeatureSetResponse) \| Create or update and existing feature set.

This function is idempotent - it will not create a new feature set if schema does not change. If an existing feature set is updated, core will advance the version number, which will be returned in response. \| \| UpdateStore \| [UpdateStoreRequest](proto.md#UpdateStoreRequest) \| [UpdateStoreResponse](proto.md#UpdateStoreResponse) \| Updates core with the configuration of the store.

If the changes are valid, core will return the given store configuration in response, and start or update the necessary feature population jobs for the updated store. \|

### ApplyFeatureSetRequest

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| feature\_set | [FeatureSetSpec](proto.md#feast.core.FeatureSetSpec) |  | Feature set version and source will be ignored |

### ApplyFeatureSetResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| feature\_set | [FeatureSetSpec](proto.md#feast.core.FeatureSetSpec) |  | Feature set response has been enriched with version and source information |
| status | [ApplyFeatureSetResponse.Status](proto.md#feast.core.ApplyFeatureSetResponse.Status) |  |  |

### GetFeastCoreVersionRequest

### GetFeastCoreVersionResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| version | [string](proto.md#string) |  |  |

### GetFeatureSetsRequest

Retrieves details for all versions of a specific feature set

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| filter | [GetFeatureSetsRequest.Filter](proto.md#feast.core.GetFeatureSetsRequest.Filter) |  |  |

### GetFeatureSetsRequest.Filter

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| feature\_set\_name | [string](proto.md#string) |  | Name of the desired feature set. Valid regex strings are allowed. e.g. - . _can be used to match all feature sets - my-project-._ can be used to match all features prefixed by "my-project" |
| feature\_set\_version | [string](proto.md#string) |  | Version of the desired feature set. Either a number or valid expression can be provided. e.g. - 1 will match version 1 exactly - &gt;=1 will match all versions greater or equal to 1 - &lt;10 will match all versions less than 10 |

### GetFeatureSetsResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| feature\_sets | [FeatureSetSpec](proto.md#feast.core.FeatureSetSpec) | repeated |  |

### GetStoresRequest

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| filter | [GetStoresRequest.Filter](proto.md#feast.core.GetStoresRequest.Filter) |  |  |

### GetStoresRequest.Filter

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](proto.md#string) |  | Name of desired store. Regex is not supported in this query. |

### GetStoresResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| store | [Store](proto.md#feast.core.Store) | repeated |  |

### UpdateStoreRequest

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| store | [Store](proto.md#feast.core.Store) |  |  |

### UpdateStoreResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| store | [Store](proto.md#feast.core.Store) |  |  |
| status | [UpdateStoreResponse.Status](proto.md#feast.core.UpdateStoreResponse.Status) |  |  |

### ApplyFeatureSetResponse.Status

| Name | Number | Description |
| :--- | :--- | :--- |
| NO\_CHANGE | 0 | Latest feature set version is consistent with provided feature set |
| CREATED | 1 | New feature set or feature set version created |
| ERROR | 2 | Error occurred while trying to apply changes |

### UpdateStoreResponse.Status

| Name | Number | Description |
| :--- | :--- | :--- |
| NO\_CHANGE | 0 | Existing store config matching the given store id is identical to the given store config. |
| UPDATED | 1 | New store created or existing config updated. |

[Top](proto.md#top)

## feast.core.FeatureSet.proto

### EntitySpec

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](proto.md#string) |  | Name of the entity. |
| value\_type | [feast.types.ValueType.Enum](proto.md#feast.types.ValueType.Enum) |  | Value type of the feature. |

### FeatureSetSpec

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](proto.md#string) |  | Name of the featureSet. Must be unique. |
| version | [int32](proto.md#int32) |  | FeatureSet version. |
| entities | [EntitySpec](proto.md#feast.core.EntitySpec) | repeated | List of entities contained within this featureSet. This allows the feature to be used during joins between feature sets. If the featureSet is ingested into a store that supports keys, this value will be made a key. |
| features | [FeatureSpec](proto.md#feast.core.FeatureSpec) | repeated | List of features contained within this featureSet. |
| max\_age | [google.protobuf.Duration](proto.md#google.protobuf.Duration) |  | Features in this feature set will only be retrieved if they are found after \[time - max\_age\]. Missing or older feature values will be returned as nulls and indicated to end user |
| source | [Source](proto.md#feast.core.Source) |  | Source on which feature rows can be found |

### FeatureSpec

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](proto.md#string) |  | Name of the feature. |
| value\_type | [feast.types.ValueType.Enum](proto.md#feast.types.ValueType.Enum) |  | Value type of the feature. |

[Top](proto.md#top)

## feast.core.Source.proto

### KafkaSourceConfig

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| bootstrap\_servers | [string](proto.md#string) |  | - bootstrapServers: \[comma delimited value of host\[:port\]\] |
| topic | [string](proto.md#string) |  | - topics: \[Kafka topic name. This value is provisioned by core and should not be set by the user.\] |

### Source

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| type | [SourceType](proto.md#feast.core.SourceType) |  | The kind of data source Feast should connect to in order to retrieve FeatureRow value |
| kafka\_source\_config | [KafkaSourceConfig](proto.md#feast.core.KafkaSourceConfig) |  |  |

### SourceType

| Name | Number | Description |
| :--- | :--- | :--- |
| INVALID | 0 |  |
| KAFKA | 1 |  |

[Top](proto.md#top)

## feast.core.Store.proto

### Store

Store provides a location where Feast reads and writes feature values. Feature values will be written to the Store in the form of FeatureRow elements. The way FeatureRow is encoded and decoded when it is written to and read from the Store depends on the type of the Store.

For example, a FeatureRow will materialize as a row in a table in BigQuery but it will materialize as a key, value pair element in Redis.

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](proto.md#string) |  | Name of the store. |
| type | [Store.StoreType](proto.md#feast.core.Store.StoreType) |  | Type of store. |
| subscriptions | [Store.Subscription](proto.md#feast.core.Store.Subscription) | repeated | Feature sets to subscribe to. |
| redis\_config | [Store.RedisConfig](proto.md#feast.core.Store.RedisConfig) |  |  |
| bigquery\_config | [Store.BigQueryConfig](proto.md#feast.core.Store.BigQueryConfig) |  |  |
| cassandra\_config | [Store.CassandraConfig](proto.md#feast.core.Store.CassandraConfig) |  |  |

### Store.BigQueryConfig

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| project\_id | [string](proto.md#string) |  |  |
| dataset\_id | [string](proto.md#string) |  |  |

### Store.CassandraConfig

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| host | [string](proto.md#string) |  |  |
| port | [int32](proto.md#int32) |  |  |

### Store.RedisConfig

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| host | [string](proto.md#string) |  |  |
| port | [int32](proto.md#int32) |  |  |

### Store.Subscription

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](proto.md#string) |  | Name of featureSet to subscribe to. |
| version | [string](proto.md#string) |  | Versions of the given featureSet that will be ingested into this store. Valid options for version: latest: only subscribe to latest version of feature set \[version number\]: pin to a specific version &gt;\[version number\]: subscribe to all versions larger than or equal to \[version number\] |

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

[Top](proto.md#top)

## feast.serving.ServingService.proto

### ServingService

| Method Name | Request Type | Response Type | Description |
| :--- | :--- | :--- | :--- |
| GetFeastServingInfo | [GetFeastServingInfoRequest](proto.md#GetFeastServingInfoRequest) | [GetFeastServingInfoResponse](proto.md#GetFeastServingInfoResponse) | Get information about this Feast serving. |
| GetOnlineFeatures | [GetOnlineFeaturesRequest](proto.md#GetOnlineFeaturesRequest) | [GetOnlineFeaturesResponse](proto.md#GetOnlineFeaturesResponse) | Get online features synchronously. |
| GetBatchFeatures | [GetBatchFeaturesRequest](proto.md#GetBatchFeaturesRequest) | [GetBatchFeaturesResponse](proto.md#GetBatchFeaturesResponse) | Get batch features asynchronously. |

The client should check the status of the returned job periodically by calling ReloadJob to determine if the job has completed successfully or with an error. If the job completes successfully i.e. status = JOB\_STATUS\_DONE with no error, then the client can check the file\_uris for the location to download feature values data. The client is assumed to have access to these file URIs. \| \| GetJob \| [GetJobRequest](proto.md#GetJobRequest) \| [GetJobResponse](proto.md#GetJobResponse) \| Get the latest job status for batch feature retrieval. \|

### DatasetSource

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| file\_source | [DatasetSource.FileSource](proto.md#feast.serving.DatasetSource.FileSource) |  | File source to load the dataset from. |

### DatasetSource.FileSource

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| file\_uris | [string](proto.md#string) | repeated | URIs to retrieve the dataset from, e.g. gs://bucket/directory/object.csv. Wildcards are supported. This data must be compatible to be uploaded to the serving store, and also be accessible by this serving instance. |
| data\_format | [DataFormat](proto.md#feast.serving.DataFormat) |  | Format of the data. Currently only avro is supported. |

### FeatureSet

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](proto.md#string) |  | Feature set name |
| version | [int32](proto.md#int32) |  | Feature set version |
| feature\_names | [string](proto.md#string) | repeated | Features that should be retrieved from this feature set |
| max\_age | [google.protobuf.Duration](proto.md#google.protobuf.Duration) |  | The features will be retrieved if: entity\_timestamp - max\_age &lt;= event\_timestamp &lt;= entity\_timestamp |

If unspecified the default max\_age specified in FeatureSetSpec will be used. \|

### GetBatchFeaturesRequest

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| feature\_sets | [FeatureSet](proto.md#feast.serving.FeatureSet) | repeated | List of feature sets and their features that are being retrieved. |
| dataset\_source | [DatasetSource](proto.md#feast.serving.DatasetSource) |  | Source of the entity dataset containing the timestamps and entity keys to retrieve features for. |

### GetBatchFeaturesResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| job | [Job](proto.md#feast.serving.Job) |  |  |

### GetFeastServingInfoRequest

### GetFeastServingInfoResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| version | [string](proto.md#string) |  | Feast version of this serving deployment. |
| type | [FeastServingType](proto.md#feast.serving.FeastServingType) |  | Type of serving deployment, either ONLINE or BATCH. Different store types support different feature retrieval methods. |
| job\_staging\_location | [string](proto.md#string) |  | Note: Batch specific options start from 10. Staging location for this serving store, if any. |

### GetJobRequest

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| job | [Job](proto.md#feast.serving.Job) |  |  |

### GetJobResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| job | [Job](proto.md#feast.serving.Job) |  |  |

### GetOnlineFeaturesRequest

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| feature\_sets | [FeatureSet](proto.md#feast.serving.FeatureSet) | repeated | List of feature sets and their features that are being retrieved |
| entity\_rows | [GetOnlineFeaturesRequest.EntityRow](proto.md#feast.serving.GetOnlineFeaturesRequest.EntityRow) | repeated | List of entity rows, containing entity id and timestamp data. Used during retrieval of feature rows and for joining feature rows into a final dataset |
| omit\_entities\_in\_response | [bool](proto.md#bool) |  | Option to omit entities from the response. If true, only feature values will be returned. |

### GetOnlineFeaturesRequest.EntityRow

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| entity\_timestamp | [google.protobuf.Timestamp](proto.md#google.protobuf.Timestamp) |  | Request timestamp of this row. This value will be used, together with maxAge, to determine feature staleness. |
| fields | [GetOnlineFeaturesRequest.EntityRow.FieldsEntry](proto.md#feast.serving.GetOnlineFeaturesRequest.EntityRow.FieldsEntry) | repeated | Map containing mapping of entity name to entity value. |

### GetOnlineFeaturesRequest.EntityRow.FieldsEntry

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| key | [string](proto.md#string) |  |  |
| value | [feast.types.Value](proto.md#feast.types.Value) |  |  |

### GetOnlineFeaturesResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| field\_values | [GetOnlineFeaturesResponse.FieldValues](proto.md#feast.serving.GetOnlineFeaturesResponse.FieldValues) | repeated | Feature values retrieved from feast. |

### GetOnlineFeaturesResponse.FieldValues

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| fields | [GetOnlineFeaturesResponse.FieldValues.FieldsEntry](proto.md#feast.serving.GetOnlineFeaturesResponse.FieldValues.FieldsEntry) | repeated | Map of feature or entity name to feature/entity values. Timestamps are not returned in this response. |

### GetOnlineFeaturesResponse.FieldValues.FieldsEntry

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| key | [string](proto.md#string) |  |  |
| value | [feast.types.Value](proto.md#feast.types.Value) |  |  |

### Job

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| id | [string](proto.md#string) |  |  |
| type | [JobType](proto.md#feast.serving.JobType) |  | Output only. The type of the job. |
| status | [JobStatus](proto.md#feast.serving.JobStatus) |  | Output only. Current state of the job. |
| error | [string](proto.md#string) |  | Output only. If not empty, the job has failed with this error message. |
| file\_uris | [string](proto.md#string) | repeated | Output only. The list of URIs for the files to be downloaded or uploaded \(depends on the job type\) for this particular job. |
| data\_format | [DataFormat](proto.md#feast.serving.DataFormat) |  | Output only. The data format for all the files. For CSV format, the files contain both feature values and a column header. |

### DataFormat

| Name | Number | Description |
| :--- | :--- | :--- |
| DATA\_FORMAT\_INVALID | 0 |  |
| DATA\_FORMAT\_CSV | 1 |  |
| DATA\_FORMAT\_PARQUET | 2 |  |
| DATA\_FORMAT\_AVRO | 3 |  |
| DATA\_FORMAT\_JSON | 4 |  |

### FeastServingType

| Name | Number | Description |
| :--- | :--- | :--- |
| FEAST\_SERVING\_TYPE\_INVALID | 0 |  |
| FEAST\_SERVING\_TYPE\_ONLINE | 1 | Online serving receives entity data directly and synchronously and will respond immediately. |
| FEAST\_SERVING\_TYPE\_BATCH | 2 | Batch serving receives entity data asynchronously and orchestrates the retrieval through a staging location. |

### JobStatus

| Name | Number | Description |
| :--- | :--- | :--- |
| JOB\_STATUS\_INVALID | 0 |  |
| JOB\_STATUS\_PENDING | 1 |  |
| JOB\_STATUS\_RUNNING | 2 |  |
| JOB\_STATUS\_DONE | 3 |  |

### JobType

| Name | Number | Description |
| :--- | :--- | :--- |
| JOB\_TYPE\_INVALID | 0 |  |
| JOB\_TYPE\_DOWNLOAD | 1 |  |

[Top](proto.md#top)

## feast.storage.Redis.proto

### RedisKey

Field number 1 is reserved for a future distributing hash if needed \(for when redis is clustered\).

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| feature\_set | [string](proto.md#string) |  | FeatureSet this row belongs to, this is defined as featureSetName:version. |
| entities | [feast.types.Field](proto.md#feast.types.Field) | repeated | List of fields containing entity names and their respective values contained within this feature row. |

[Top](proto.md#top)

## feast.types.FeatureRow.proto

### FeatureRow

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| fields | [Field](proto.md#feast.types.Field) | repeated | Fields in the feature row. |
| event\_timestamp | [google.protobuf.Timestamp](proto.md#google.protobuf.Timestamp) |  | Timestamp of the feature row. While the actual definition of this timestamp may vary depending on the upstream feature creation pipelines, this is the timestamp that Feast will use to perform joins, determine latest values, and coalesce rows. |
| feature\_set | [string](proto.md#string) |  | Complete reference to the featureSet this featureRow belongs to, in the form of featureSetName:version. This value will be used by the feast ingestion job to filter rows, and write the values to the correct tables. |

[Top](proto.md#top)

## feast.types.FeatureRowExtended.proto

### Attempt

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| attempts | [int32](proto.md#int32) |  |  |
| error | [Error](proto.md#feast.types.Error) |  |  |

### Error

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| cause | [string](proto.md#string) |  | exception class name |
| transform | [string](proto.md#string) |  | name of transform where the error occurred |
| message | [string](proto.md#string) |  |  |
| stack\_trace | [string](proto.md#string) |  |  |

### FeatureRowExtended

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| row | [FeatureRow](proto.md#feast.types.FeatureRow) |  |  |
| last\_attempt | [Attempt](proto.md#feast.types.Attempt) |  |  |
| first\_seen | [google.protobuf.Timestamp](proto.md#google.protobuf.Timestamp) |  |  |

[Top](proto.md#top)

## feast.types.Field.proto

### Field

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](proto.md#string) |  |  |
| value | [Value](proto.md#feast.types.Value) |  |  |

[Top](proto.md#top)

## feast.types.Value.proto

### BoolList

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [bool](proto.md#bool) | repeated |  |

### BytesList

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [bytes](proto.md#bytes) | repeated |  |

### DoubleList

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [double](proto.md#double) | repeated |  |

### FloatList

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [float](proto.md#float) | repeated |  |

### Int32List

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [int32](proto.md#int32) | repeated |  |

### Int64List

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [int64](proto.md#int64) | repeated |  |

### StringList

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [string](proto.md#string) | repeated |  |

### Value

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| bytes\_val | [bytes](proto.md#bytes) |  |  |
| string\_val | [string](proto.md#string) |  |  |
| int32\_val | [int32](proto.md#int32) |  |  |
| int64\_val | [int64](proto.md#int64) |  |  |
| double\_val | [double](proto.md#double) |  |  |
| float\_val | [float](proto.md#float) |  |  |
| bool\_val | [bool](proto.md#bool) |  |  |
| bytes\_list\_val | [BytesList](proto.md#feast.types.BytesList) |  |  |
| string\_list\_val | [StringList](proto.md#feast.types.StringList) |  |  |
| int32\_list\_val | [Int32List](proto.md#feast.types.Int32List) |  |  |
| int64\_list\_val | [Int64List](proto.md#feast.types.Int64List) |  |  |
| double\_list\_val | [DoubleList](proto.md#feast.types.DoubleList) |  |  |
| float\_list\_val | [FloatList](proto.md#feast.types.FloatList) |  |  |
| bool\_list\_val | [BoolList](proto.md#feast.types.BoolList) |  |  |

### ValueType

### ValueType.Enum

| Name | Number | Description |
| :--- | :--- | :--- |
| INVALID | 0 |  |
| BYTES | 1 |  |
| STRING | 2 |  |
| INT32 | 3 |  |
| INT64 | 4 |  |
| DOUBLE | 5 |  |
| FLOAT | 6 |  |
| BOOL | 7 |  |
| BYTES\_LIST | 11 |  |
| STRING\_LIST | 12 |  |
| INT32\_LIST | 13 |  |
| INT64\_LIST | 14 |  |
| DOUBLE\_LIST | 15 |  |
| FLOAT\_LIST | 16 |  |
| BOOL\_LIST | 17 |  |

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

