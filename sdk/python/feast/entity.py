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
from datetime import datetime
from json import dumps
from typing import Dict, List, Optional

from google.protobuf.json_format import MessageToJson
from pydantic import BaseModel, root_validator
from typeguard import typechecked

from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.Entity_pb2 import EntityMeta as EntityMetaProto
from feast.protos.feast.core.Entity_pb2 import EntitySpecV2 as EntitySpecProto
from feast.usage import log_exceptions
from feast.value_type import ValueType


@typechecked
class Entity(BaseModel):
    """
    An entity defines a collection of entities for which features can be defined. An
    entity can also contain associated metadata.

    Attributes:
        name: The unique name of the entity.
        value_type: The type of the entity, such as string or float.
        join_key: A property that uniquely identifies different entities within the
            collection. The join_key property is typically used for joining entities
            with their associated features. If not specified, defaults to the name.
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the entity, typically the email of the primary maintainer.
        created_timestamp: The time when the entity was created.
        last_updated_timestamp: The time when the entity was last updated.
    """

    name: str
    value_type: Optional[ValueType] = None
    join_key: str
    description: str = ""
    tags: Optional[Dict[str, str]] = None
    owner: str = ""
    created_timestamp: Optional[datetime]
    last_updated_timestamp: Optional[datetime]

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"
        json_encoders = {
            ValueType: lambda v: int(dumps(v.value, default=str))
        }

    @root_validator(pre=True)
    def validate_and_adjust_fields(cls, values):
        try:
            values["value_type"] = values.get("value_type", ValueType.UNKNOWN)
            values["tags"] = values.get("tags", {})
            values["created_timestamp"] = None
            values["last_updated_timestamp"] = None

            # Replace join_keys with a single join_key
            join_keys = values.get("join_keys", None)
            if join_keys and len(join_keys) > 1:
                # TODO(felixwang9817): When multiple join keys are supported, add a `join_keys` attribute
                # and deprecate the `join_key` attribute.
                raise ValueError(
                    "An entity may only have a single join key. "
                    "Multiple join keys will be supported in the future."
                )
            elif join_keys and len(join_keys) == 1:
                values["join_key"] = join_keys[0]
            else:
                values["join_key"] = values["name"]

            if "join_keys" in values:
                del values["join_keys"]

            return values
        except KeyError as exception:
            raise TypeError("Entity missing required values.") from exception

    def __hash__(self) -> int:
        return hash((self.name, self.join_key))

    def __eq__(self, other):
        if not isinstance(other, Entity):
            raise TypeError("Comparisons should only involve Entity class objects.")

        if (
            self.name != other.name
            or self.value_type != other.value_type
            or self.join_key != other.join_key
            or self.description != other.description
            or self.tags != other.tags
            or self.owner != other.owner
        ):
            return False

        return True

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __lt__(self, other):
        return self.name < other.name

    def is_valid(self):
        """
        Validates the state of this entity locally.

        Raises:
            ValueError: The entity does not have a name or does not have a type.
        """
        if not self.name:
            raise ValueError("The entity does not have a name.")

        if not self.value_type:
            raise ValueError(f"The entity {self.name} does not have a type.")

    @classmethod
    def from_proto(cls, entity_proto: EntityProto):
        """
        Creates an entity from a protobuf representation of an entity.

        Args:
            entity_proto: A protobuf representation of an entity.

        Returns:
            An Entity object based on the entity protobuf.
        """
        entity = cls(
            name=entity_proto.spec.name,
            join_keys=[entity_proto.spec.join_key],
            description=entity_proto.spec.description,
            tags=dict(entity_proto.spec.tags),
            owner=entity_proto.spec.owner,
        )

        entity.value_type = ValueType(entity_proto.spec.value_type)

        if entity_proto.meta.HasField("created_timestamp"):
            entity.created_timestamp = entity_proto.meta.created_timestamp.ToDatetime()
        if entity_proto.meta.HasField("last_updated_timestamp"):
            entity.last_updated_timestamp = (
                entity_proto.meta.last_updated_timestamp.ToDatetime()
            )

        return entity

    def to_proto(self) -> EntityProto:
        """
        Converts an entity object to its protobuf representation.

        Returns:
            An EntityProto protobuf.
        """
        meta = EntityMetaProto()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)
        if self.last_updated_timestamp:
            meta.last_updated_timestamp.FromDatetime(self.last_updated_timestamp)

        spec = EntitySpecProto(
            name=self.name,
            value_type=self.value_type.value,
            join_key=self.join_key,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        return EntityProto(spec=spec, meta=meta)
