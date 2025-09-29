# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from enum import IntEnum
from google.protobuf.message import Message  # type: ignore
from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator
import typing

class Null(IntEnum):
    NULL = 0

class ValueType(BaseModel):
    class Enum(IntEnum):
        INVALID = 0
        BYTES = 1
        STRING = 2
        INT32 = 3
        INT64 = 4
        DOUBLE = 5
        FLOAT = 6
        BOOL = 7
        UNIX_TIMESTAMP = 8
        BYTES_LIST = 11
        STRING_LIST = 12
        INT32_LIST = 13
        INT64_LIST = 14
        DOUBLE_LIST = 15
        FLOAT_LIST = 16
        BOOL_LIST = 17
        UNIX_TIMESTAMP_LIST = 18
        NULL = 19

class BytesList(BaseModel):
    val: typing.List[bytes] = Field(default_factory=list)

class StringList(BaseModel):
    val: typing.List[str] = Field(default_factory=list)

class Int32List(BaseModel):
    val: typing.List[int] = Field(default_factory=list)

class Int64List(BaseModel):
    val: typing.List[int] = Field(default_factory=list)

class DoubleList(BaseModel):
    val: typing.List[float] = Field(default_factory=list)

class FloatList(BaseModel):
    val: typing.List[float] = Field(default_factory=list)

class BoolList(BaseModel):
    val: typing.List[bool] = Field(default_factory=list)

class Value(BaseModel):
    _one_of_dict = {"Value.val": {"fields": {"bool_list_val", "bool_val", "bytes_list_val", "bytes_val", "double_list_val", "double_val", "float_list_val", "float_val", "int32_list_val", "int32_val", "int64_list_val", "int64_val", "null_val", "string_list_val", "string_val", "unix_timestamp_list_val", "unix_timestamp_val"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
    model_config = ConfigDict(validate_default=True)
    bytes_val: bytes = Field(default=b"")
    string_val: str = Field(default="")
    int32_val: int = Field(default=0)
    int64_val: int = Field(default=0)
    double_val: float = Field(default=0.0)
    float_val: float = Field(default=0.0)
    bool_val: bool = Field(default=False)
    unix_timestamp_val: int = Field(default=0)
    bytes_list_val: BytesList = Field(default_factory=BytesList)
    string_list_val: StringList = Field(default_factory=StringList)
    int32_list_val: Int32List = Field(default_factory=Int32List)
    int64_list_val: Int64List = Field(default_factory=Int64List)
    double_list_val: DoubleList = Field(default_factory=DoubleList)
    float_list_val: FloatList = Field(default_factory=FloatList)
    bool_list_val: BoolList = Field(default_factory=BoolList)
    unix_timestamp_list_val: Int64List = Field(default_factory=Int64List)
    null_val: Null = Field(default=0)

class RepeatedValue(BaseModel):
    """
     This is to avoid an issue of being unable to specify `repeated value` in oneofs or maps
 In JSON "val" field can be omitted
    """

    val: typing.List[Value] = Field(default_factory=list)
