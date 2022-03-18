# Copyright 2019 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import enum
from typing import Type, Union

from feast.proto_types.Value_pb2 import (
    BoolList,
    BytesList,
    DoubleList,
    FloatList,
    Int32List,
    Int64List,
    StringList,
)
from feast.proto_types.Value_pb2 import ValueType as ProtoValueType

ListType = Union[
    Type[BoolList],
    Type[BytesList],
    Type[DoubleList],
    Type[FloatList],
    Type[Int32List],
    Type[Int64List],
    Type[StringList],
]


class ValueType(enum.Enum):
    """
    Feature value type. Used to define data types in Feature Tables.
    """

    UNKNOWN = 0
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


_value_type_proto_value_type_mapping = {
    ValueType.BYTES: ProtoValueType.BYTES,
    ValueType.STRING: ProtoValueType.STRING,
    ValueType.INT32: ProtoValueType.INT32,
    ValueType.INT64: ProtoValueType.INT64,
    ValueType.DOUBLE: ProtoValueType.DOUBLE,
    ValueType.FLOAT: ProtoValueType.FLOAT,
    ValueType.BOOL: ProtoValueType.BOOL,
    ValueType.UNIX_TIMESTAMP: ProtoValueType.UNIX_TIMESTAMP,
    ValueType.BYTES_LIST: ProtoValueType.BYTES_LIST,
    ValueType.STRING_LIST: ProtoValueType.STRING_LIST,
    ValueType.INT32_LIST: ProtoValueType.INT32_LIST,
    ValueType.INT64_LIST: ProtoValueType.INT64_LIST,
    ValueType.DOUBLE_LIST: ProtoValueType.DOUBLE_LIST,
    ValueType.FLOAT_LIST: ProtoValueType.FLOAT_LIST,
    ValueType.BOOL_LIST: ProtoValueType.BOOL_LIST,
    ValueType.NULL: ProtoValueType.NULL,
}


def value_type_to_proto_value_type(value_type: ValueType,):
    return _value_type_proto_value_type_mapping[value_type]
    pass
