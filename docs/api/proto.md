# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [feast.core.CoreService.proto](#feast-core-coreservice-proto)
    - [CoreService](#coreservice)
  
    - [ApplyFeatureSetRequest](#applyfeaturesetrequest)
    - [ApplyFeatureSetResponse](#applyfeaturesetresponse)
    - [GetFeastCoreVersionRequest](#getfeastcoreversionrequest)
    - [GetFeastCoreVersionResponse](#getfeastcoreversionresponse)
    - [GetFeatureSetsRequest](#getfeaturesetsrequest)
    - [GetFeatureSetsRequest.Filter](#filter)
    - [GetFeatureSetsResponse](#getfeaturesetsresponse)
    - [GetStoresRequest](#getstoresrequest)
    - [GetStoresRequest.Filter](#filter)
    - [GetStoresResponse](#getstoresresponse)
  
    - [ApplyFeatureSetResponse.Status](#status)
  
  

- [feast.core.FeatureSet.proto](#feast-core-featureset-proto)
  
    - [EntitySpec](#entityspec)
    - [FeatureSetSpec](#featuresetspec)
    - [FeatureSpec](#featurespec)
  
  
  

- [feast.core.Source.proto](#feast-core-source-proto)
  
    - [KafkaSourceConfig](#kafkasourceconfig)
    - [Source](#source)
  
    - [SourceType](#sourcetype)
  
  

- [feast.core.Store.proto](#feast-core-store-proto)
  
    - [Store](#store)
    - [Store.BigQueryConfig](#bigqueryconfig)
    - [Store.CassandraConfig](#cassandraconfig)
    - [Store.RedisConfig](#redisconfig)
    - [Store.Subscription](#subscription)
  
    - [Store.StoreType](#storetype)
  
  

- [feast.serving.ServingService.proto](#feast-serving-servingservice-proto)
    - [ServingService](#servingservice)
  
    - [GetBatchFeaturesResponse](#getbatchfeaturesresponse)
    - [GetFeastServingTypeRequest](#getfeastservingtyperequest)
    - [GetFeastServingTypeResponse](#getfeastservingtyperesponse)
    - [GetFeastServingVersionRequest](#getfeastservingversionrequest)
    - [GetFeastServingVersionResponse](#getfeastservingversionresponse)
    - [GetFeaturesRequest](#getfeaturesrequest)
    - [GetFeaturesRequest.EntityRow](#entityrow)
    - [GetFeaturesRequest.EntityRow.FieldsEntry](#fieldsentry)
    - [GetFeaturesRequest.FeatureSet](#featureset)
    - [GetJobRequest](#getjobrequest)
    - [GetJobResponse](#getjobresponse)
    - [GetOnlineFeaturesResponse](#getonlinefeaturesresponse)
    - [GetOnlineFeaturesResponse.FieldValues](#fieldvalues)
    - [GetOnlineFeaturesResponse.FieldValues.FieldsEntry](#fieldsentry)
    - [Job](#job)
  
    - [DataFormat](#dataformat)
    - [FeastServingType](#feastservingtype)
    - [JobStatus](#jobstatus)
    - [JobType](#jobtype)
  
  

- [feast.storage.Redis.proto](#feast-storage-redis-proto)
  
    - [RedisKey](#rediskey)
  
  
  

- [feast.types.FeatureRow.proto](#feast-types-featurerow-proto)
  
    - [FeatureRow](#featurerow)
  
  
  

- [feast.types.FeatureRowExtended.proto](#feast-types-featurerowextended-proto)
  
    - [Attempt](#attempt)
    - [Error](#error)
    - [FeatureRowExtended](#featurerowextended)
  
  
  

- [feast.types.Field.proto](#feast-types-field-proto)
  
    - [Field](#field)
  
  
  

- [feast.types.Value.proto](#feast-types-value-proto)
  
    - [BoolList](#boollist)
    - [BytesList](#byteslist)
    - [DoubleList](#doublelist)
    - [FloatList](#floatlist)
    - [Int32List](#int32list)
    - [Int64List](#int64list)
    - [StringList](#stringlist)
    - [Value](#value)
    - [ValueType](#valuetype)
  
    - [ValueType.Enum](#enum)
  
  

- [Scalar Value Types](#scalar-value-types)



<a name="feast-core-coreservice-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast.core.CoreService.proto



<a name="feast.core.CoreService"></a>

### CoreService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetFeastCoreVersion | [GetFeastCoreVersionRequest](#GetFeastCoreVersionRequest) | [GetFeastCoreVersionResponse](#GetFeastCoreVersionResponse) | Retrieve version information about this Feast deployment |
| GetFeatureSets | [GetFeatureSetsRequest](#GetFeatureSetsRequest) | [GetFeatureSetsResponse](#GetFeatureSetsResponse) | Retrieve feature set details given a filter. Returns all featureSets matching that filter. |
| GetStores | [GetStoresRequest](#GetStoresRequest) | [GetStoresResponse](#GetStoresResponse) | Retrieve store details given a filter. Returns all stores matching that filter. |
| ApplyFeatureSet | [ApplyFeatureSetRequest](#ApplyFeatureSetRequest) | [ApplyFeatureSetResponse](#ApplyFeatureSetResponse) | Idempotent creation of feature set. Will not create a new feature set if schema does not change |

 <!-- end services -->


<a name="feast-core-applyfeaturesetrequest"></a>

### ApplyFeatureSetRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_set | [FeatureSetSpec](#feast.core.FeatureSetSpec) |  | Feature set version and source will be ignored |






<a name="feast-core-applyfeaturesetresponse"></a>

### ApplyFeatureSetResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_set | [FeatureSetSpec](#feast.core.FeatureSetSpec) |  | Feature set response has been enriched with version and source information |
| status | [ApplyFeatureSetResponse.Status](#feast.core.ApplyFeatureSetResponse.Status) |  |  |






<a name="feast-core-getfeastcoreversionrequest"></a>

### GetFeastCoreVersionRequest







<a name="feast-core-getfeastcoreversionresponse"></a>

### GetFeastCoreVersionResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [string](#string) |  |  |






<a name="feast-core-getfeaturesetsrequest"></a>

### GetFeatureSetsRequest
Retrieves details for all versions of a specific feature set


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filter | [GetFeatureSetsRequest.Filter](#feast.core.GetFeatureSetsRequest.Filter) |  |  |






<a name="feast-core-getfeaturesetsrequest-filter"></a>

### GetFeatureSetsRequest.Filter



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_set_name | [string](#string) |  |  |
| feature_set_version | [string](#string) |  |  |






<a name="feast-core-getfeaturesetsresponse"></a>

### GetFeatureSetsResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_sets | [FeatureSetSpec](#feast.core.FeatureSetSpec) | repeated |  |






<a name="feast-core-getstoresrequest"></a>

### GetStoresRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| filter | [GetStoresRequest.Filter](#feast.core.GetStoresRequest.Filter) |  |  |






<a name="feast-core-getstoresrequest-filter"></a>

### GetStoresRequest.Filter



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |






<a name="feast-core-getstoresresponse"></a>

### GetStoresResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| store | [Store](#feast.core.Store) | repeated |  |





 <!-- end messages -->


<a name="feast-core-applyfeaturesetresponse-status"></a>

### ApplyFeatureSetResponse.Status


| Name | Number | Description |
| ---- | ------ | ----------- |
| NO_CHANGE | 0 | Latest feature set version is consistent with provided feature set |
| CREATED | 1 | New feature set or feature set version created |
| ERROR | 2 | Error occurred while trying to apply changes |


 <!-- end enums -->

 <!-- end HasExtensions -->





<a name="feast-core-featureset-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast.core.FeatureSet.proto


 <!-- end services -->


<a name="feast-core-entityspec"></a>

### EntitySpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the entity. |
| value_type | [feast.types.ValueType.Enum](#feast.types.ValueType.Enum) |  | Value type of the feature. |






<a name="feast-core-featuresetspec"></a>

### FeatureSetSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the featureSet. Must be unique. |
| version | [int32](#int32) |  | FeatureSet version. |
| entities | [EntitySpec](#feast.core.EntitySpec) | repeated | List of entities contained within this featureSet. This allows the feature to be used during joins between feature sets. If the featureSet is ingested into a store that supports keys, this value will be made a key. |
| features | [FeatureSpec](#feast.core.FeatureSpec) | repeated | List of features contained within this featureSet. |
| max_age | [google.protobuf.Duration](#google.protobuf.Duration) |  | Features in this feature set will only be retrieved if they are found after [time - max_age]. Missing or older feature values will be returned as nulls and indicated to end user |
| source | [Source](#feast.core.Source) |  | Source on which feature rows can be found |






<a name="feast-core-featurespec"></a>

### FeatureSpec



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of the feature. |
| value_type | [feast.types.ValueType.Enum](#feast.types.ValueType.Enum) |  | Value type of the feature. |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->





<a name="feast-core-source-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast.core.Source.proto


 <!-- end services -->


<a name="feast-core-kafkasourceconfig"></a>

### KafkaSourceConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bootstrap_servers | [string](#string) |  | - bootstrapServers: [comma delimited value of host[:port]] |
| topic | [string](#string) |  | - topics: [Kafka topic name. This value is provisioned by core and should not be set by the user.] |






<a name="feast-core-source"></a>

### Source



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [SourceType](#feast.core.SourceType) |  | The kind of data source Feast should connect to in order to retrieve FeatureRow value |
| kafka_source_config | [KafkaSourceConfig](#feast.core.KafkaSourceConfig) |  |  |





 <!-- end messages -->


<a name="feast-core-sourcetype"></a>

### SourceType


| Name | Number | Description |
| ---- | ------ | ----------- |
| INVALID | 0 |  |
| KAFKA | 1 |  |


 <!-- end enums -->

 <!-- end HasExtensions -->





<a name="feast-core-store-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast.core.Store.proto


 <!-- end services -->


<a name="feast-core-store"></a>

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






<a name="feast-core-store-bigqueryconfig"></a>

### Store.BigQueryConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| project_id | [string](#string) |  |  |
| dataset_id | [string](#string) |  |  |






<a name="feast-core-store-cassandraconfig"></a>

### Store.CassandraConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host | [string](#string) |  |  |
| port | [int32](#int32) |  |  |






<a name="feast-core-store-redisconfig"></a>

### Store.RedisConfig



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| host | [string](#string) |  |  |
| port | [int32](#int32) |  |  |






<a name="feast-core-store-subscription"></a>

### Store.Subscription



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Name of featureSet to subscribe to. |
| version | [string](#string) |  | Versions of the given featureSet that will be ingested into this store. Valid options for version: latest: only subscribe to latest version of feature set [version number]: pin to a specific version >[version number]: subscribe to all versions larger than or equal to [version number] |





 <!-- end messages -->


<a name="feast-core-store-storetype"></a>

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





<a name="feast-serving-servingservice-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast.serving.ServingService.proto



<a name="feast.serving.ServingService"></a>

### ServingService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetFeastServingVersion | [GetFeastServingVersionRequest](#GetFeastServingVersionRequest) | [GetFeastServingVersionResponse](#GetFeastServingVersionResponse) | Get version information about this Feast serving. |
| GetFeastServingType | [GetFeastServingTypeRequest](#GetFeastServingTypeRequest) | [GetFeastServingTypeResponse](#GetFeastServingTypeResponse) | Get Feast serving store type: online or batch. |
| GetOnlineFeatures | [GetFeaturesRequest](#GetFeaturesRequest) | [GetOnlineFeaturesResponse](#GetOnlineFeaturesResponse) | Get online features synchronously. |
| GetBatchFeatures | [GetFeaturesRequest](#GetFeaturesRequest) | [GetBatchFeaturesResponse](#GetBatchFeaturesResponse) | Get batch features asynchronously. 

The client should check the status of the returned job periodically by calling ReloadJob to determine if the job has completed successfully or with an error. If the job completes successfully i.e. status = JOB_STATUS_DONE with no error, then the client can check the file_uris for the location to download feature values data. The client is assumed to have access to these file URIs. |
| GetJob | [GetJobRequest](#GetJobRequest) | [GetJobResponse](#GetJobResponse) | Get the latest job status for batch feature retrieval. |

 <!-- end services -->


<a name="feast-serving-getbatchfeaturesresponse"></a>

### GetBatchFeaturesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job | [Job](#feast.serving.Job) |  |  |






<a name="feast-serving-getfeastservingtyperequest"></a>

### GetFeastServingTypeRequest







<a name="feast-serving-getfeastservingtyperesponse"></a>

### GetFeastServingTypeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [FeastServingType](#feast.serving.FeastServingType) |  |  |






<a name="feast-serving-getfeastservingversionrequest"></a>

### GetFeastServingVersionRequest







<a name="feast-serving-getfeastservingversionresponse"></a>

### GetFeastServingVersionResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [string](#string) |  |  |






<a name="feast-serving-getfeaturesrequest"></a>

### GetFeaturesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_sets | [GetFeaturesRequest.FeatureSet](#feast.serving.GetFeaturesRequest.FeatureSet) | repeated | List of feature sets and their features that are being retrieved |
| entity_rows | [GetFeaturesRequest.EntityRow](#feast.serving.GetFeaturesRequest.EntityRow) | repeated | List of entity rows, containing entity id and timestamp data. Used during retrieval of feature rows and for joining feature rows into a final dataset |






<a name="feast-serving-getfeaturesrequest-entityrow"></a>

### GetFeaturesRequest.EntityRow



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entity_timestamp | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  | Request timestamp of this row. This value will be used, together with maxAge, to determine feature staleness. |
| fields | [GetFeaturesRequest.EntityRow.FieldsEntry](#feast.serving.GetFeaturesRequest.EntityRow.FieldsEntry) | repeated | Map containing mapping of entity name to entity value. |






<a name="feast-serving-getfeaturesrequest-entityrow-fieldsentry"></a>

### GetFeaturesRequest.EntityRow.FieldsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [feast.types.Value](#feast.types.Value) |  |  |






<a name="feast-serving-getfeaturesrequest-featureset"></a>

### GetFeaturesRequest.FeatureSet



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Feature set name |
| version | [int32](#int32) |  | Feature set version |
| feature_names | [string](#string) | repeated | Features that should be retrieved from this feature set |
| max_age | [google.protobuf.Duration](#google.protobuf.Duration) |  | The features will be retrieved if: entity_timestamp - max_age <= event_timestamp <= entity_timestamp

If unspecified the default max_age specified in FeatureSetSpec will be used. |






<a name="feast-serving-getjobrequest"></a>

### GetJobRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job | [Job](#feast.serving.Job) |  |  |






<a name="feast-serving-getjobresponse"></a>

### GetJobResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job | [Job](#feast.serving.Job) |  |  |






<a name="feast-serving-getonlinefeaturesresponse"></a>

### GetOnlineFeaturesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| field_values | [GetOnlineFeaturesResponse.FieldValues](#feast.serving.GetOnlineFeaturesResponse.FieldValues) | repeated |  |






<a name="feast-serving-getonlinefeaturesresponse-fieldvalues"></a>

### GetOnlineFeaturesResponse.FieldValues
TODO: update this comment
does not include timestamp, includes features and entities


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| fields | [GetOnlineFeaturesResponse.FieldValues.FieldsEntry](#feast.serving.GetOnlineFeaturesResponse.FieldValues.FieldsEntry) | repeated |  |






<a name="feast-serving-getonlinefeaturesresponse-fieldvalues-fieldsentry"></a>

### GetOnlineFeaturesResponse.FieldValues.FieldsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [feast.types.Value](#feast.types.Value) |  |  |






<a name="feast-serving-job"></a>

### Job



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  |  |
| type | [JobType](#feast.serving.JobType) |  | Output only. The type of the job. |
| status | [JobStatus](#feast.serving.JobStatus) |  | Output only. Current state of the job. |
| error | [string](#string) |  | Output only. If not empty, the job has failed with this error message. |
| file_uris | [string](#string) | repeated | Output only. The list of URIs for the files to be downloaded or uploaded (depends on the job type) for this particular job. |
| data_format | [DataFormat](#feast.serving.DataFormat) |  | Output only. The data format for all the files. For CSV format, the files contain both feature values and a column header. |





 <!-- end messages -->


<a name="feast-serving-dataformat"></a>

### DataFormat


| Name | Number | Description |
| ---- | ------ | ----------- |
| DATA_FORMAT_INVALID | 0 |  |
| DATA_FORMAT_CSV | 1 |  |
| DATA_FORMAT_PARQUET | 2 |  |
| DATA_FORMAT_AVRO | 3 |  |
| DATA_FORMAT_JSON | 4 |  |



<a name="feast-serving-feastservingtype"></a>

### FeastServingType


| Name | Number | Description |
| ---- | ------ | ----------- |
| FEAST_SERVING_TYPE_INVALID | 0 |  |
| FEAST_SERVING_TYPE_ONLINE | 1 | Online serving receives entity data directly and synchronously and will respond immediately. |
| FEAST_SERVING_TYPE_BATCH | 2 | Batch serving receives entity data asynchronously and orchestrates the retrieval through a staging location. |



<a name="feast-serving-jobstatus"></a>

### JobStatus


| Name | Number | Description |
| ---- | ------ | ----------- |
| JOB_STATUS_INVALID | 0 |  |
| JOB_STATUS_PENDING | 1 |  |
| JOB_STATUS_RUNNING | 2 |  |
| JOB_STATUS_DONE | 3 |  |



<a name="feast-serving-jobtype"></a>

### JobType


| Name | Number | Description |
| ---- | ------ | ----------- |
| JOB_TYPE_INVALID | 0 |  |
| JOB_TYPE_DOWNLOAD | 1 |  |


 <!-- end enums -->

 <!-- end HasExtensions -->





<a name="feast-storage-redis-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast.storage.Redis.proto


 <!-- end services -->


<a name="feast-storage-rediskey"></a>

### RedisKey
Field number 1 is reserved for a future distributing hash if needed
(for when redis is clustered).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_set | [string](#string) |  | FeatureSet this row belongs to, this is defined as featureSetName:version. |
| entities | [feast.types.Field](#feast.types.Field) | repeated | List of fields containing entity names and their respective values contained within this feature row. |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->





<a name="feast-types-featurerow-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast.types.FeatureRow.proto


 <!-- end services -->


<a name="feast-types-featurerow"></a>

### FeatureRow



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| fields | [Field](#feast.types.Field) | repeated | Fields in the feature row. |
| event_timestamp | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  | Timestamp of the feature row. While the actual definition of this timestamp may vary depending on the upstream feature creation pipelines, this is the timestamp that Feast will use to perform joins, determine latest values, and coalesce rows. |
| feature_set | [string](#string) |  | Complete reference to the featureSet this featureRow belongs to, in the form of featureSetName:version. This value will be used by the feast ingestion job to filter rows, and write the values to the correct tables. |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->





<a name="feast-types-featurerowextended-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast.types.FeatureRowExtended.proto


 <!-- end services -->


<a name="feast-types-attempt"></a>

### Attempt



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| attempts | [int32](#int32) |  |  |
| error | [Error](#feast.types.Error) |  |  |






<a name="feast-types-error"></a>

### Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cause | [string](#string) |  | exception class name |
| transform | [string](#string) |  | name of transform where the error occurred |
| message | [string](#string) |  |  |
| stack_trace | [string](#string) |  |  |






<a name="feast-types-featurerowextended"></a>

### FeatureRowExtended



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| row | [FeatureRow](#feast.types.FeatureRow) |  |  |
| last_attempt | [Attempt](#feast.types.Attempt) |  |  |
| first_seen | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  |  |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->





<a name="feast-types-field-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast.types.Field.proto


 <!-- end services -->


<a name="feast-types-field"></a>

### Field



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| value | [Value](#feast.types.Value) |  |  |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->





<a name="feast-types-value-proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast.types.Value.proto


 <!-- end services -->


<a name="feast-types-boollist"></a>

### BoolList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [bool](#bool) | repeated |  |






<a name="feast-types-byteslist"></a>

### BytesList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [bytes](#bytes) | repeated |  |






<a name="feast-types-doublelist"></a>

### DoubleList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [double](#double) | repeated |  |






<a name="feast-types-floatlist"></a>

### FloatList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [float](#float) | repeated |  |






<a name="feast-types-int32list"></a>

### Int32List



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [int32](#int32) | repeated |  |






<a name="feast-types-int64list"></a>

### Int64List



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [int64](#int64) | repeated |  |






<a name="feast-types-stringlist"></a>

### StringList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [string](#string) | repeated |  |






<a name="feast-types-value"></a>

### Value



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| bytes_val | [bytes](#bytes) |  |  |
| string_val | [string](#string) |  |  |
| int32_val | [int32](#int32) |  |  |
| int64_val | [int64](#int64) |  |  |
| double_val | [double](#double) |  |  |
| float_val | [float](#float) |  |  |
| bool_val | [bool](#bool) |  |  |
| bytes_list_val | [BytesList](#feast.types.BytesList) |  |  |
| string_list_val | [StringList](#feast.types.StringList) |  |  |
| int32_list_val | [Int32List](#feast.types.Int32List) |  |  |
| int64_list_val | [Int64List](#feast.types.Int64List) |  |  |
| double_list_val | [DoubleList](#feast.types.DoubleList) |  |  |
| float_list_val | [FloatList](#feast.types.FloatList) |  |  |
| bool_list_val | [BoolList](#feast.types.BoolList) |  |  |






<a name="feast-types-valuetype"></a>

### ValueType






 <!-- end messages -->


<a name="feast-types-valuetype-enum"></a>

### ValueType.Enum


| Name | Number | Description |
| ---- | ------ | ----------- |
| INVALID | 0 |  |
| BYTES | 1 |  |
| STRING | 2 |  |
| INT32 | 3 |  |
| INT64 | 4 |  |
| DOUBLE | 5 |  |
| FLOAT | 6 |  |
| BOOL | 7 |  |
| BYTES_LIST | 11 |  |
| STRING_LIST | 12 |  |
| INT32_LIST | 13 |  |
| INT64_LIST | 14 |  |
| DOUBLE_LIST | 15 |  |
| FLOAT_LIST | 16 |  |
| BOOL_LIST | 17 |  |


 <!-- end enums -->

 <!-- end HasExtensions -->





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
