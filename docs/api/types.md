# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [feast/types/FeatureRow.proto](#feast-types-featurerow-proto)
    - [FeatureRow](#feast-types-featurerow)
  
  
  
  

- [feast/types/FeatureRowExtended.proto](#feast-types-featurerowextended-proto)
    - [Attempt](#feast-types-attempt)
    - [Error](#feast-types-error)
    - [FeatureRowExtended](#feast-types-featurerowextended)
  
  
  
  

- [feast/types/Field.proto](#feast-types-field-proto)
    - [Field](#feast-types-field)
  
  
  
  

- [feast/types/Value.proto](#feast-types-value-proto)
    - [BoolList](#feast-types-boollist)
    - [BytesList](#feast-types-byteslist)
    - [DoubleList](#feast-types-doublelist)
    - [FloatList](#feast-types-floatlist)
    - [Int32List](#feast-types-int32list)
    - [Int64List](#feast-types-int64list)
    - [StringList](#feast-types-stringlist)
    - [Value](#feast-types-value)
    - [ValueType](#feast-types-valuetype)
  
    - [ValueType.Enum](#feast-types-valuetype-enum)
  
  
  

- [Scalar Value Types](#scalar-value-types)



<a name="feast/types/FeatureRow.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast/types/FeatureRow.proto



<a name="feast.types.FeatureRow"></a>

### FeatureRow



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| fields | [Field](#feast.types.Field) | repeated | Fields in the feature row. |
| event_timestamp | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  | Timestamp of the feature row. While the actual definition of this timestamp may vary depending on the upstream feature creation pipelines, this is the timestamp that Feast will use to perform joins, determine latest values, and coalesce rows. |
| feature_set | [string](#string) |  | Complete reference to the featureSet this featureRow belongs to, in the form of featureSetName:version. This value will be used by the feast ingestion job to filter rows, and write the values to the correct tables. |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->

 <!-- end services -->



<a name="feast/types/FeatureRowExtended.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast/types/FeatureRowExtended.proto



<a name="feast.types.Attempt"></a>

### Attempt



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| attempts | [int32](#int32) |  |  |
| error | [Error](#feast.types.Error) |  |  |






<a name="feast.types.Error"></a>

### Error



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| cause | [string](#string) |  | exception class name |
| transform | [string](#string) |  | name of transform where the error occurred |
| message | [string](#string) |  |  |
| stack_trace | [string](#string) |  |  |






<a name="feast.types.FeatureRowExtended"></a>

### FeatureRowExtended



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| row | [FeatureRow](#feast.types.FeatureRow) |  |  |
| last_attempt | [Attempt](#feast.types.Attempt) |  |  |
| first_seen | [google.protobuf.Timestamp](#google.protobuf.Timestamp) |  |  |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->

 <!-- end services -->



<a name="feast/types/Field.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast/types/Field.proto



<a name="feast.types.Field"></a>

### Field



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| name | [string](#string) |  |  |
| value | [Value](#feast.types.Value) |  |  |





 <!-- end messages -->

 <!-- end enums -->

 <!-- end HasExtensions -->

 <!-- end services -->



<a name="feast/types/Value.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast/types/Value.proto



<a name="feast.types.BoolList"></a>

### BoolList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [bool](#bool) | repeated |  |






<a name="feast.types.BytesList"></a>

### BytesList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [bytes](#bytes) | repeated |  |






<a name="feast.types.DoubleList"></a>

### DoubleList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [double](#double) | repeated |  |






<a name="feast.types.FloatList"></a>

### FloatList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [float](#float) | repeated |  |






<a name="feast.types.Int32List"></a>

### Int32List



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [int32](#int32) | repeated |  |






<a name="feast.types.Int64List"></a>

### Int64List



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [int64](#int64) | repeated |  |






<a name="feast.types.StringList"></a>

### StringList



| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| val | [string](#string) | repeated |  |






<a name="feast.types.Value"></a>

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






<a name="feast.types.ValueType"></a>

### ValueType






 <!-- end messages -->


<a name="feast.types.ValueType.Enum"></a>

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
