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

from feast.core.FeatureSet_pb2 import EntitySpec as EntityProto
from feast.field import Field
from feast.types import Value_pb2 as ValueTypeProto
from feast.value_type import ValueType


class Entity(Field):
    """Entity field type"""

    def to_proto(self) -> EntityProto:
        """
        Converts Entity to its Protocol Buffer representation

        Returns:
            Returns EntitySpec object
        """
        value_type = ValueTypeProto.ValueType.Enum.Value(self.dtype.name)
        return EntityProto(
            name=self.name,
            value_type=value_type,
            presence=self.presence,
            group_presence=self.group_presence,
            shape=self.shape,
            value_count=self.value_count,
            domain=self.domain,
            int_domain=self.int_domain,
            float_domain=self.float_domain,
            string_domain=self.string_domain,
            bool_domain=self.bool_domain,
            struct_domain=self.struct_domain,
            natural_language_domain=self.natural_language_domain,
            image_domain=self.image_domain,
            mid_domain=self.mid_domain,
            url_domain=self.url_domain,
            time_domain=self.time_domain,
            time_of_day_domain=self.time_of_day_domain,
        )

    @classmethod
    def from_proto(cls, entity_proto: EntityProto):
        """
        Creates a Feast Entity object from its Protocol Buffer representation

        Args:
            entity_proto: EntitySpec protobuf object

        Returns:
            Entity object
        """
        entity = cls(name=entity_proto.name, dtype=ValueType(entity_proto.value_type))
        entity.update_presence_constraints(entity_proto)
        entity.update_shape_type(entity_proto)
        entity.update_domain_info(entity_proto)
        return entity
