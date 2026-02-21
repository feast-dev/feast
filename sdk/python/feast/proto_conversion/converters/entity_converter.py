# Copyright 2024 The Feast Authors
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

"""
EntityConverter for bidirectional Entity <-> EntityProto conversion.

This module provides a clean, type-safe converter for Entity objects,
demonstrating the pattern that can be applied to other Feast objects.
"""

from typing import Type

from feast.entity import Entity
from feast.proto_conversion.converter import ProtoConverter
from feast.proto_conversion.errors import DeserializationError, SerializationError
from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.Entity_pb2 import EntityMeta as EntityMetaProto
from feast.protos.feast.core.Entity_pb2 import EntitySpecV2 as EntitySpecProto
from feast.value_type import ValueType


class EntityConverter(ProtoConverter[Entity, EntityProto]):
    """
    Converter for Entity <-> EntityProto conversion.

    This converter handles bidirectional conversion between Entity
    Python objects and their protobuf representation (EntityProto).

    Example:
        >>> converter = EntityConverter()
        >>> entity = Entity(name="user", join_keys=["user_id"], value_type=ValueType.INT64)
        >>> proto = converter.to_proto(entity)
        >>> restored = converter.from_proto(proto)
        >>> assert entity == restored
    """

    @property
    def supported_type(self) -> Type[Entity]:
        return Entity

    @property
    def proto_type(self) -> Type[EntityProto]:
        return EntityProto

    def to_proto(self, obj: Entity) -> EntityProto:
        """
        Convert an Entity to its protobuf representation.

        Args:
            obj: The Entity object to convert.

        Returns:
            The EntityProto representation.

        Raises:
            SerializationError: If conversion fails.
        """
        try:
            meta = EntityMetaProto()
            if obj.created_timestamp:
                meta.created_timestamp.FromDatetime(obj.created_timestamp)
            if obj.last_updated_timestamp:
                meta.last_updated_timestamp.FromDatetime(obj.last_updated_timestamp)

            spec = EntitySpecProto(
                name=obj.name,
                value_type=obj.value_type.value,
                join_key=obj.join_key,
                description=obj.description,
                tags=obj.tags,
                owner=obj.owner,
            )

            return EntityProto(spec=spec, meta=meta)

        except Exception as e:
            raise SerializationError(obj, EntityProto, cause=e) from e

    def from_proto(self, proto: EntityProto) -> Entity:
        """
        Convert an EntityProto to an Entity object.

        Args:
            proto: The EntityProto to convert.

        Returns:
            The Entity object.

        Raises:
            DeserializationError: If conversion fails.
        """
        try:
            entity = Entity(
                name=proto.spec.name,
                join_keys=[proto.spec.join_key],
                value_type=ValueType(proto.spec.value_type),
                description=proto.spec.description,
                tags=dict(proto.spec.tags),
                owner=proto.spec.owner,
            )

            if proto.meta.HasField("created_timestamp"):
                entity.created_timestamp = proto.meta.created_timestamp.ToDatetime()
            if proto.meta.HasField("last_updated_timestamp"):
                entity.last_updated_timestamp = (
                    proto.meta.last_updated_timestamp.ToDatetime()
                )

            return entity

        except Exception as e:
            raise DeserializationError(proto, Entity, cause=e) from e
