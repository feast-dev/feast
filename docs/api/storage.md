# Protocol Documentation

## Table of Contents

* [feast/storage/Redis.proto](storage.md#feast/storage/Redis.proto)
  * [RedisKey](storage.md#feast.storage.RedisKey)
* [Scalar Value Types](storage.md#scalar-value-types)

[Top](storage.md#top)

## feast/storage/Redis.proto

### RedisKey

Field number 1 is reserved for a future distributing hash if needed \(for when redis is clustered\).

| Field | Type | Label | Description |
| :--- | :--- | :--- | :--- |
| feature\_set | [string](storage.md#string) |  | FeatureSet this row belongs to, this is defined as featureSetName:version. |
| entities | [feast.types.Field](storage.md#feast.types.Field) | repeated | List of fields containing entity names and their respective values contained within this feature row. |

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

