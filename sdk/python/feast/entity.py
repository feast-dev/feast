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
from feast.core.FeatureSet_pb2 import EntitySpec as EntityProto
from feast.types import Value_pb2 as ValueTypeProto
from feast.field import Field


class Entity(Field):
    def to_proto(self) -> EntityProto:
        value_type = ValueTypeProto.ValueType.Enum.Value(self.dtype.name)
        return EntityProto(name=self.name, value_type=value_type)

    @classmethod
    def from_proto(cls, entity_proto: EntityProto):
        return cls(name=entity_proto.name, dtype=ValueType(entity_proto.value_type))
