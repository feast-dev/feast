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

from feast.protos.feast.types.Value_pb2 import (
    BoolList,
    BoolSet,
    BytesList,
    BytesSet,
    DoubleList,
    DoubleSet,
    FloatList,
    FloatSet,
    Int32List,
    Int32Set,
    Int64List,
    Int64Set,
    StringList,
    StringSet,
)


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
    MAP = 20
    MAP_LIST = 21
    BYTES_SET = 22
    STRING_SET = 23
    INT32_SET = 24
    INT64_SET = 25
    DOUBLE_SET = 26
    FLOAT_SET = 27
    BOOL_SET = 28
    UNIX_TIMESTAMP_SET = 29
    PDF_BYTES = 30
    IMAGE_BYTES = 31
    UUID = 32
    TIME_UUID = 33
    UUID_LIST = 34
    TIME_UUID_LIST = 35


ListType = Union[
    Type[BoolList],
    Type[BytesList],
    Type[DoubleList],
    Type[FloatList],
    Type[Int32List],
    Type[Int64List],
    Type[StringList],
]

SetType = Union[
    Type[BoolSet],
    Type[BytesSet],
    Type[DoubleSet],
    Type[FloatSet],
    Type[Int32Set],
    Type[Int64Set],
    Type[StringSet],
]
