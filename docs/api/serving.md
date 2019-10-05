# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [feast/serving/ServingService.proto](#feast/serving/ServingService.proto)
    - [GetBatchFeaturesResponse](#feast.serving.GetBatchFeaturesResponse)
    - [GetFeastServingTypeRequest](#feast.serving.GetFeastServingTypeRequest)
    - [GetFeastServingTypeResponse](#feast.serving.GetFeastServingTypeResponse)
    - [GetFeastServingVersionRequest](#feast.serving.GetFeastServingVersionRequest)
    - [GetFeastServingVersionResponse](#feast.serving.GetFeastServingVersionResponse)
    - [GetFeaturesRequest](#feast.serving.GetFeaturesRequest)
    - [GetFeaturesRequest.EntityRow](#feast.serving.GetFeaturesRequest.EntityRow)
    - [GetFeaturesRequest.EntityRow.FieldsEntry](#feast.serving.GetFeaturesRequest.EntityRow.FieldsEntry)
    - [GetFeaturesRequest.FeatureSet](#feast.serving.GetFeaturesRequest.FeatureSet)
    - [GetOnlineFeaturesResponse](#feast.serving.GetOnlineFeaturesResponse)
    - [GetOnlineFeaturesResponse.FieldValues](#feast.serving.GetOnlineFeaturesResponse.FieldValues)
    - [GetOnlineFeaturesResponse.FieldValues.FieldsEntry](#feast.serving.GetOnlineFeaturesResponse.FieldValues.FieldsEntry)
    - [Job](#feast.serving.Job)
    - [ReloadJobRequest](#feast.serving.ReloadJobRequest)
    - [ReloadJobResponse](#feast.serving.ReloadJobResponse)
  
    - [DataFormat](#feast.serving.DataFormat)
    - [FeastServingType](#feast.serving.FeastServingType)
    - [JobStatus](#feast.serving.JobStatus)
    - [JobType](#feast.serving.JobType)
  
  
    - [ServingService](#feast.serving.ServingService)
  

- [Scalar Value Types](#scalar-value-types)



<a name="feast/serving/ServingService.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast/serving/ServingService.proto



<a name="feast.serving.GetBatchFeaturesResponse"></a>

### GetBatchFeaturesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job | [Job](#feast.serving.Job) |  |  |






<a name="feast.serving.GetFeastServingTypeRequest"></a>

### GetFeastServingTypeRequest







<a name="feast.serving.GetFeastServingTypeResponse"></a>

### GetFeastServingTypeResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| type | [FeastServingType](#feast.serving.FeastServingType) |  |  |






<a name="feast.serving.GetFeastServingVersionRequest"></a>

### GetFeastServingVersionRequest







<a name="feast.serving.GetFeastServingVersionResponse"></a>

### GetFeastServingVersionResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| version | [string](#string) |  |  |






<a name="feast.serving.GetFeaturesRequest"></a>

### GetFeaturesRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_sets | [GetFeaturesRequest.FeatureSet](#feast.serving.GetFeaturesRequest.FeatureSet) | repeated | List of feature sets and their features that are being retrieved |
| entity_rows | [GetFeaturesRequest.EntityRow](#feast.serving.GetFeaturesRequest.EntityRow) | repeated | List of entity rows, containing entity id and timestamp data. Used during retrieval of feature rows and for joining feature rows into a final dataset |






<a name="feast.serving.GetFeaturesRequest.EntityRow"></a>

### GetFeaturesRequest.EntityRow



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| entity_timestamp | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  | Request timestamp of this row. This value will be used, together with maxAge, to determine feature staleness. |
| fields | [GetFeaturesRequest.EntityRow.FieldsEntry](#feast.serving.GetFeaturesRequest.EntityRow.FieldsEntry) | repeated | Map containing mapping of entity name to entity value. |






<a name="feast.serving.GetFeaturesRequest.EntityRow.FieldsEntry"></a>

### GetFeaturesRequest.EntityRow.FieldsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [feast.types.Value](#feast.types.Value) |  |  |






<a name="feast.serving.GetFeaturesRequest.FeatureSet"></a>

### GetFeaturesRequest.FeatureSet



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  | Feature set name |
| version | [int32](#int32) |  | Feature set version |
| feature_names | [string](#string) | repeated | Features that should be retrieved from this feature set |
| max_age | [google.protobuf.Duration](#google.protobuf.Duration) |  | The features will be retrieved if: entity_timestamp - max_age &lt;= event_timestamp &lt;= entity_timestamp

If unspecified the default max_age specified in FeatureSetSpec will be used. |






<a name="feast.serving.GetOnlineFeaturesResponse"></a>

### GetOnlineFeaturesResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| field_values | [GetOnlineFeaturesResponse.FieldValues](#feast.serving.GetOnlineFeaturesResponse.FieldValues) | repeated |  |






<a name="feast.serving.GetOnlineFeaturesResponse.FieldValues"></a>

### GetOnlineFeaturesResponse.FieldValues
TODO: update this comment
does not include timestamp, includes features and entities


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| fields | [GetOnlineFeaturesResponse.FieldValues.FieldsEntry](#feast.serving.GetOnlineFeaturesResponse.FieldValues.FieldsEntry) | repeated |  |






<a name="feast.serving.GetOnlineFeaturesResponse.FieldValues.FieldsEntry"></a>

### GetOnlineFeaturesResponse.FieldValues.FieldsEntry



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| key | [string](#string) |  |  |
| value | [feast.types.Value](#feast.types.Value) |  |  |






<a name="feast.serving.Job"></a>

### Job



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| id | [string](#string) |  |  |
| type | [JobType](#feast.serving.JobType) |  | Output only. The type of the job. |
| status | [JobStatus](#feast.serving.JobStatus) |  | Output only. Current state of the job. |
| error | [string](#string) |  | Output only. If not empty, the job has failed with this error message. |
| file_uris | [string](#string) | repeated | Output only. The list of URIs for the files to be downloaded or uploaded (depends on the job type) for this particular job. |
| data_format | [DataFormat](#feast.serving.DataFormat) |  | Output only. The data format for all the files. For CSV format, the files contain both feature values and a column header. |






<a name="feast.serving.ReloadJobRequest"></a>

### ReloadJobRequest



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job | [Job](#feast.serving.Job) |  |  |






<a name="feast.serving.ReloadJobResponse"></a>

### ReloadJobResponse



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| job | [Job](#feast.serving.Job) |  |  |





 


<a name="feast.serving.DataFormat"></a>

### DataFormat


| Name | Number | Description |
| ---- | ------ | ----------- |
| DATA_FORMAT_INVALID | 0 |  |
| DATA_FORMAT_CSV | 1 |  |
| DATA_FORMAT_PARQUET | 2 |  |
| DATA_FORMAT_AVRO | 3 |  |
| DATA_FORMAT_JSON | 4 |  |



<a name="feast.serving.FeastServingType"></a>

### FeastServingType


| Name | Number | Description |
| ---- | ------ | ----------- |
| FEAST_SERVING_TYPE_INVALID | 0 |  |
| FEAST_SERVING_TYPE_ONLINE | 1 | Online serving receives entity data directly and synchronously and will respond immediately. |
| FEAST_SERVING_TYPE_BATCH | 2 | Batch serving receives entity data asynchronously and orchestrates the retrieval through a staging location. |



<a name="feast.serving.JobStatus"></a>

### JobStatus


| Name | Number | Description |
| ---- | ------ | ----------- |
| JOB_STATUS_INVALID | 0 |  |
| JOB_STATUS_PENDING | 1 |  |
| JOB_STATUS_RUNNING | 2 |  |
| JOB_STATUS_DONE | 3 |  |



<a name="feast.serving.JobType"></a>

### JobType


| Name | Number | Description |
| ---- | ------ | ----------- |
| JOB_TYPE_INVALID | 0 |  |
| JOB_TYPE_DOWNLOAD | 1 |  |


 

 


<a name="feast.serving.ServingService"></a>

### ServingService


| Method Name | Request Type | Response Type | Description |
| ----------- | ------------ | ------------- | ------------|
| GetFeastServingVersion | [GetFeastServingVersionRequest](#feast.serving.GetFeastServingVersionRequest) | [GetFeastServingVersionResponse](#feast.serving.GetFeastServingVersionResponse) | Get version information about this Feast serving. |
| GetFeastServingType | [GetFeastServingTypeRequest](#feast.serving.GetFeastServingTypeRequest) | [GetFeastServingTypeResponse](#feast.serving.GetFeastServingTypeResponse) | Get Feast serving store type: online or batch. |
| GetOnlineFeatures | [GetFeaturesRequest](#feast.serving.GetFeaturesRequest) | [GetOnlineFeaturesResponse](#feast.serving.GetOnlineFeaturesResponse) | Get online features synchronously. |
| GetBatchFeatures | [GetFeaturesRequest](#feast.serving.GetFeaturesRequest) | [GetBatchFeaturesResponse](#feast.serving.GetBatchFeaturesResponse) | Get batch features asynchronously. 

The client should check the status of the returned job periodically by calling ReloadJob to determine if the job has completed successfully or with an error. If the job completes successfully i.e. status = JOB_STATUS_DONE with no error, then the client can check the file_uris for the location to download feature values data. The client is assumed to have access to these file URIs. |
| ReloadJob | [ReloadJobRequest](#feast.serving.ReloadJobRequest) | [ReloadJobResponse](#feast.serving.ReloadJobResponse) | Reload the job status with the latest state. |

 



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

