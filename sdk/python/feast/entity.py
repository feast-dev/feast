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
import warnings
from datetime import datetime
from typing import Dict, List, Optional

from google.protobuf.json_format import MessageToJson
from typeguard import typechecked

from feast.protos.feast.core.Entity_pb2 import Entity as EntityProto
from feast.protos.feast.core.Entity_pb2 import EntityMeta as EntityMetaProto
from feast.protos.feast.core.Entity_pb2 import EntitySpecV2 as EntitySpecProto
from feast.value_type import ValueType


@typechecked
class Entity:
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
    value_type: ValueType
    join_key: str
    description: str
    tags: Dict[str, str]
    owner: str
    created_timestamp: Optional[datetime]
    last_updated_timestamp: Optional[datetime]

    def __init__(
        self,
        *,
        name: str,
        join_keys: Optional[List[str]] = None,
        value_type: Optional[ValueType] = None,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
    ):
        """
        Creates an Entity object.

        Args:
            name: The unique name of the entity.
            join_keys (optional): A list of properties that uniquely identifies different entities
                within the collection. This currently only supports a list of size one, but is
                intended to eventually support multiple join keys.
            value_type (optional): The type of the entity, such as string or float. If not specified,
                it will be inferred from the schema of the underlying data source.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the entity, typically the email of the primary maintainer.

        Raises:
            ValueError: Parameters are specified incorrectly.
        """
        self.name = name
        if value_type is None:
            warnings.warn(
                "Entity value_type will be mandatory in the next release. "
                "Please specify a value_type for entity '%s'." % name,
                DeprecationWarning,
                stacklevel=2,
            )
        self.value_type = value_type or ValueType.UNKNOWN

        if join_keys and len(join_keys) > 1:
            # TODO(felixwang9817): When multiple join keys are supported, add a `join_keys` attribute
            # and deprecate the `join_key` attribute.
            raise ValueError(
                "An entity may only have a single join key. "
                "Multiple join keys will be supported in the future."
            )
        elif join_keys and len(join_keys) == 1:
            self.join_key = join_keys[0]
        else:
            self.join_key = self.name

        self.description = description
        self.tags = tags if tags is not None else {}
        self.owner = owner
        self.created_timestamp = None
        self.last_updated_timestamp = None

    def __repr__(self):
        return (
            f"Entity(\n"
            f"    name={self.name!r},\n"
            f"    value_type={self.value_type!r},\n"
            f"    join_key={self.join_key!r},\n"
            f"    description={self.description!r},\n"
            f"    tags={self.tags!r},\n"
            f"    owner={self.owner!r},\n"
            f"    created_timestamp={self.created_timestamp!r},\n"
            f"    last_updated_timestamp={self.last_updated_timestamp!r}\n"
            f")"
        )

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
            value_type=ValueType(entity_proto.spec.value_type),
            description=entity_proto.spec.description,
            tags=dict(entity_proto.spec.tags),
            owner=entity_proto.spec.owner,
        )

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
