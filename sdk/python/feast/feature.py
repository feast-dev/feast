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
from feast.field import Field


class Feature(Field):
    """Feature field type"""

    def to_proto(self) -> FeatureProto:
        """Converts Feature object to its Protocol Buffer representation"""
        value_type = ValueTypeProto.ValueType.Enum.Value(self.dtype.name)
        return FeatureProto(name=self.name, value_type=value_type)

    @classmethod
    def from_proto(cls, feature_proto: FeatureProto):
        """Converts Protobuf Feature to its SDK equivalent"""
        return cls(name=feature_proto.name, dtype=ValueType(feature_proto.value_type))
