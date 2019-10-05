# Protocol Documentation

## Table of Contents

* [feast/types/FeatureRow.proto](types.md#feast/types/FeatureRow.proto)
  * [FeatureRow](types.md#feast.types.FeatureRow)
* [feast/types/FeatureRowExtended.proto](types.md#feast/types/FeatureRowExtended.proto)
  * [Attempt](types.md#feast.types.Attempt)
  * [Error](types.md#feast.types.Error)
  * [FeatureRowExtended](types.md#feast.types.FeatureRowExtended)
* [feast/types/Field.proto](types.md#feast/types/Field.proto)
  * [Field](types.md#feast.types.Field)
* [feast/types/Value.proto](types.md#feast/types/Value.proto)
  * [BoolList](types.md#feast.types.BoolList)
  * [BytesList](types.md#feast.types.BytesList)
  * [DoubleList](types.md#feast.types.DoubleList)
  * [FloatList](types.md#feast.types.FloatList)
  * [Int32List](types.md#feast.types.Int32List)
  * [Int64List](types.md#feast.types.Int64List)
  * [StringList](types.md#feast.types.StringList)
  * [Value](types.md#feast.types.Value)
  * [ValueType](types.md#feast.types.ValueType)
  * [ValueType.Enum](types.md#feast.types.ValueType.Enum)
* [Scalar Value Types](types.md#scalar-value-types)

[Top](types.md#top)

## feast/types/FeatureRow.proto

### FeatureRow

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| fields | [Field](types.md#feast.types.Field) | repeated | Fields in the feature row. |
| event\_timestamp | [google.protobuf.Timestamp](types.md#google.protobuf.Timestamp) |  | Timestamp of the feature row. While the actual definition of this timestamp may vary depending on the upstream feature creation pipelines, this is the timestamp that Feast will use to perform joins, determine latest values, and coalesce rows. |
| feature\_set | [string](types.md#string) |  | Complete reference to the featureSet this featureRow belongs to, in the form of featureSetName:version. This value will be used by the feast ingestion job to filter rows, and write the values to the correct tables. |

[Top](types.md#top)

## feast/types/FeatureRowExtended.proto

### Attempt

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| attempts | [int32](types.md#int32) |  |  |
| error | [Error](types.md#feast.types.Error) |  |  |

### Error

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| cause | [string](types.md#string) |  | exception class name |
| transform | [string](types.md#string) |  | name of transform where the error occurred |
| message | [string](types.md#string) |  |  |
| stack\_trace | [string](types.md#string) |  |  |

### FeatureRowExtended

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| row | [FeatureRow](types.md#feast.types.FeatureRow) |  |  |
| last\_attempt | [Attempt](types.md#feast.types.Attempt) |  |  |
| first\_seen | [google.protobuf.Timestamp](types.md#google.protobuf.Timestamp) |  |  |

[Top](types.md#top)

## feast/types/Field.proto

### Field

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| name | [string](types.md#string) |  |  |
| value | [Value](types.md#feast.types.Value) |  |  |

[Top](types.md#top)

## feast/types/Value.proto

### BoolList

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [bool](types.md#bool) | repeated |  |

### BytesList

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [bytes](types.md#bytes) | repeated |  |

### DoubleList

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [double](types.md#double) | repeated |  |

### FloatList

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [float](types.md#float) | repeated |  |

### Int32List

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [int32](types.md#int32) | repeated |  |

### Int64List

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [int64](types.md#int64) | repeated |  |

### StringList

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| val | [string](types.md#string) | repeated |  |

### Value

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| bytes\_val | [bytes](types.md#bytes) |  |  |
| string\_val | [string](types.md#string) |  |  |
| int32\_val | [int32](types.md#int32) |  |  |
| int64\_val | [int64](types.md#int64) |  |  |
| double\_val | [double](types.md#double) |  |  |
| float\_val | [float](types.md#float) |  |  |
| bool\_val | [bool](types.md#bool) |  |  |
| bytes\_list\_val | [BytesList](types.md#feast.types.BytesList) |  |  |
| string\_list\_val | [StringList](types.md#feast.types.StringList) |  |  |
| int32\_list\_val | [Int32List](types.md#feast.types.Int32List) |  |  |
| int64\_list\_val | [Int64List](types.md#feast.types.Int64List) |  |  |
| double\_list\_val | [DoubleList](types.md#feast.types.DoubleList) |  |  |
| float\_list\_val | [FloatList](types.md#feast.types.FloatList) |  |  |
| bool\_list\_val | [BoolList](types.md#feast.types.BoolList) |  |  |

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

