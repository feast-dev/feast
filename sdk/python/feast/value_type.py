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

from tensorflow_metadata.proto.v0 import schema_pb2


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
    BYTES_LIST = 11
    STRING_LIST = 12
    INT32_LIST = 13
    INT64_LIST = 14
    DOUBLE_LIST = 15
    FLOAT_LIST = 16
    BOOL_LIST = 17

    def to_tfx_schema_feature_type(self):
        if self.value in [
            ValueType.BYTES.value,
            ValueType.STRING.value,
            ValueType.BOOL.value,
            ValueType.BYTES_LIST.value,
            ValueType.STRING_LIST.value,
            ValueType.INT32_LIST.value,
            ValueType.INT64_LIST.value,
            ValueType.DOUBLE_LIST.value,
            ValueType.FLOAT_LIST.value,
            ValueType.BOOL_LIST.value,
        ]:
            return schema_pb2.FeatureType.BYTES
        elif self.value in [ValueType.INT32.value, ValueType.INT64.value]:
            return schema_pb2.FeatureType.INT
        elif self.value in [ValueType.DOUBLE.value, ValueType.FLOAT.value]:
            return schema_pb2.FeatureType.FLOAT
        else:
            return schema_pb2.FeatureType.TYPE_UNKNOWN
