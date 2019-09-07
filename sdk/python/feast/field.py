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

from feast.value_type import ValueType
from feast.core.FeatureSet_pb2 import FeatureSpec as FeatureProto
from feast.types import Value_pb2 as ValueTypeProto


class Field:
    def __init__(self, name: str, dtype: ValueType):
        self._name = name
        if not isinstance(dtype, ValueType):
            raise ValueError("dtype is not a valid ValueType")
        self._dtype = dtype

    @property
    def name(self):
        return self._name

    @property
    def dtype(self) -> ValueType:
        return self._dtype

    def to_proto(self):
        pass
