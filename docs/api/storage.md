# Protocol Documentation
<a name="top"></a>

## Table of Contents

- [feast/storage/Redis.proto](#feast/storage/Redis.proto)
    - [RedisKey](#feast.storage.RedisKey)
  
  
  
  

- [Scalar Value Types](#scalar-value-types)



<a name="feast/storage/Redis.proto"></a>
<p align="right"><a href="#top">Top</a></p>

## feast/storage/Redis.proto



<a name="feast.storage.RedisKey"></a>

### RedisKey
Field number 1 is reserved for a future distributing hash if needed
(for when redis is clustered).


| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
| feature_set | [string](#string) |  | FeatureSet this row belongs to, this is defined as featureSetName:version. |
| entities | [feast.types.Field](#feast.types.Field) | repeated | List of fields containing entity names and their respective values contained within this feature row. |





 

 

 

 



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

