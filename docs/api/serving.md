# Serving Service

## Table of Contents

* [feast/serving/ServingService.proto](serving.md#feast/serving/ServingService.proto)
  * [GetBatchFeaturesResponse](serving.md#feast.serving.GetBatchFeaturesResponse)
  * [GetFeastServingTypeRequest](serving.md#feast.serving.GetFeastServingTypeRequest)
  * [GetFeastServingTypeResponse](serving.md#feast.serving.GetFeastServingTypeResponse)
  * [GetFeastServingVersionRequest](serving.md#feast.serving.GetFeastServingVersionRequest)
  * [GetFeastServingVersionResponse](serving.md#feast.serving.GetFeastServingVersionResponse)
  * [GetFeaturesRequest](serving.md#feast.serving.GetFeaturesRequest)
  * [GetFeaturesRequest.EntityRow](serving.md#feast.serving.GetFeaturesRequest.EntityRow)
  * [GetFeaturesRequest.EntityRow.FieldsEntry](serving.md#feast.serving.GetFeaturesRequest.EntityRow.FieldsEntry)
  * [GetFeaturesRequest.FeatureSet](serving.md#feast.serving.GetFeaturesRequest.FeatureSet)
  * [GetOnlineFeaturesResponse](serving.md#feast.serving.GetOnlineFeaturesResponse)
  * [GetOnlineFeaturesResponse.FieldValues](serving.md#feast.serving.GetOnlineFeaturesResponse.FieldValues)
  * [GetOnlineFeaturesResponse.FieldValues.FieldsEntry](serving.md#feast.serving.GetOnlineFeaturesResponse.FieldValues.FieldsEntry)
  * [Job](serving.md#feast.serving.Job)
  * [ReloadJobRequest](serving.md#feast.serving.ReloadJobRequest)
  * [ReloadJobResponse](serving.md#feast.serving.ReloadJobResponse)
  * [DataFormat](serving.md#feast.serving.DataFormat)
  * [FeastServingType](serving.md#feast.serving.FeastServingType)
  * [JobStatus](serving.md#feast.serving.JobStatus)
  * [JobType](serving.md#feast.serving.JobType)
* [ServingService](serving.md#feast.serving.ServingService)
* [Scalar Value Types](serving.md#scalar-value-types)

[Top](serving.md#top)

## feast/serving/ServingService.proto

### GetBatchFeaturesResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| job | [Job](serving.md#feast.serving.Job) |  |  |

### GetFeastServingTypeRequest

### GetFeastServingTypeResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| type | [FeastServingType](serving.md#feast.serving.FeastServingType) |  |  |

### GetFeastServingVersionRequest

### GetFeastServingVersionResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| version | [string](serving.md#string) |  |  |

### GetFeaturesRequest

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| feature\_sets | [GetFeaturesRequest.FeatureSet](serving.md#feast.serving.GetFeaturesRequest.FeatureSet) | repeated | List of feature sets and their features that are being retrieved |
| entity\_rows | [GetFeaturesRequest.EntityRow](serving.md#feast.serving.GetFeaturesRequest.EntityRow) | repeated | List of entity rows, containing entity id and timestamp data. Used during retrieval of feature rows and for joining feature rows into a final dataset |

### GetFeaturesRequest.EntityRow

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| entity\_timestamp | [google.protobuf.Timestamp](serving.md#google.protobuf.Timestamp) |  | Request timestamp of this row. This value will be used, together with maxAge, to determine feature staleness. |
| fields | [GetFeaturesRequest.EntityRow.FieldsEntry](serving.md#feast.serving.GetFeaturesRequest.EntityRow.FieldsEntry) | repeated | Map containing mapping of entity name to entity value. |

### GetFeaturesRequest.EntityRow.FieldsEntry

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| key | [string](serving.md#string) |  |  |
| value | [feast.types.Value](serving.md#feast.types.Value) |  |  |

### GetFeaturesRequest.FeatureSet

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](serving.md#string) |  | Feature set name |
| version | [int32](serving.md#int32) |  | Feature set version |
| feature\_names | [string](serving.md#string) | repeated | Features that should be retrieved from this feature set |
| max\_age | [google.protobuf.Duration](serving.md#google.protobuf.Duration) |  | The features will be retrieved if: entity\_timestamp - max\_age &lt;= event\_timestamp &lt;= entity\_timestamp |

If unspecified the default max\_age specified in FeatureSetSpec will be used. \|

### GetOnlineFeaturesResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| field\_values | [GetOnlineFeaturesResponse.FieldValues](serving.md#feast.serving.GetOnlineFeaturesResponse.FieldValues) | repeated |  |

### GetOnlineFeaturesResponse.FieldValues

TODO: update this comment does not include timestamp, includes features and entities

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| fields | [GetOnlineFeaturesResponse.FieldValues.FieldsEntry](serving.md#feast.serving.GetOnlineFeaturesResponse.FieldValues.FieldsEntry) | repeated |  |

### GetOnlineFeaturesResponse.FieldValues.FieldsEntry

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| key | [string](serving.md#string) |  |  |
| value | [feast.types.Value](serving.md#feast.types.Value) |  |  |

### Job

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| id | [string](serving.md#string) |  |  |
| type | [JobType](serving.md#feast.serving.JobType) |  | Output only. The type of the job. |
| status | [JobStatus](serving.md#feast.serving.JobStatus) |  | Output only. Current state of the job. |
| error | [string](serving.md#string) |  | Output only. If not empty, the job has failed with this error message. |
| file\_uris | [string](serving.md#string) | repeated | Output only. The list of URIs for the files to be downloaded or uploaded \(depends on the job type\) for this particular job. |
| data\_format | [DataFormat](serving.md#feast.serving.DataFormat) |  | Output only. The data format for all the files. For CSV format, the files contain both feature values and a column header. |

### ReloadJobRequest

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| job | [Job](serving.md#feast.serving.Job) |  |  |

### ReloadJobResponse

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| job | [Job](serving.md#feast.serving.Job) |  |  |

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

### ServingService

| Method Name | Request Type | Response Type | Description |
| :--- | :--- | :--- | :--- |
| GetFeastServingVersion | [GetFeastServingVersionRequest](serving.md#feast.serving.GetFeastServingVersionRequest) | [GetFeastServingVersionResponse](serving.md#feast.serving.GetFeastServingVersionResponse) | Get version information about this Feast serving. |
| GetFeastServingType | [GetFeastServingTypeRequest](serving.md#feast.serving.GetFeastServingTypeRequest) | [GetFeastServingTypeResponse](serving.md#feast.serving.GetFeastServingTypeResponse) | Get Feast serving store type: online or batch. |
| GetOnlineFeatures | [GetFeaturesRequest](serving.md#feast.serving.GetFeaturesRequest) | [GetOnlineFeaturesResponse](serving.md#feast.serving.GetOnlineFeaturesResponse) | Get online features synchronously. |
| GetBatchFeatures | [GetFeaturesRequest](serving.md#feast.serving.GetFeaturesRequest) | [GetBatchFeaturesResponse](serving.md#feast.serving.GetBatchFeaturesResponse) | Get batch features asynchronously. |

The client should check the status of the returned job periodically by calling ReloadJob to determine if the job has completed successfully or with an error. If the job completes successfully i.e. status = JOB\_STATUS\_DONE with no error, then the client can check the file\_uris for the location to download feature values data. The client is assumed to have access to these file URIs. \| \| ReloadJob \| [ReloadJobRequest](serving.md#feast.serving.ReloadJobRequest) \| [ReloadJobResponse](serving.md#feast.serving.ReloadJobResponse) \| Reload the job status with the latest state. \|

## Scalar Value Types

| .proto Type | Notes | C++ Type | Java Type | Python Type |
| :--- | :--- | :--- | :--- | :--- |
| double |  | double | double | float |
| float |  | float | float | float |
| int32 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint32 instead. | int32 | int | int |
| int64 | Uses variable-length encoding. Inefficient for encoding negative numbers – if your field is likely to have negative values, use sint64 instead. | int64 | long | int/long |
| uint32 | Uses variable-length encoding. | uint32 | int | int/long |
| uint64 | Uses variable-length encoding. | uint64 | long | int/long |
| sint32 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int32s. | int32 | int | int |
| sint64 | Uses variable-length encoding. Signed int value. These more efficiently encode negative numbers than regular int64s. | int64 | long | int/long |
| fixed32 | Always four bytes. More efficient than uint32 if values are often greater than 2^28. | uint32 | int | int |
| fixed64 | Always eight bytes. More efficient than uint64 if values are often greater than 2^56. | uint64 | long | int/long |
| sfixed32 | Always four bytes. | int32 | int | int |
| sfixed64 | Always eight bytes. | int64 | long | int/long |
| bool |  | bool | boolean | boolean |
| string | A string must always contain UTF-8 encoded or 7-bit ASCII text. | string | String | str/unicode |
| bytes | May contain any arbitrary sequence of bytes. | string | ByteString | str |

