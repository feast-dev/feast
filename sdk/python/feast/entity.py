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
from feast.usage import log_exceptions
from feast.value_type import ValueType


@typechecked
class Entity:
    """
    An entity defines a collection of entities for which features can be defined. An
    entity can also contain associated metadata.

    Attributes:
        name: The unique name of the entity.
        value_type (deprecated): The type of the entity, such as string or float.
        join_key: A property that uniquely identifies different entities within the
            collection. The join_key property is typically used for joining entities
            with their associated features. If not specified, defaults to the name.
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the entity, typically the email of the primary maintainer.
        created_timestamp: The time when the entity was created.
        last_updated_timestamp: The time when the entity was last updated.
        join_keys: A list of properties that uniquely identifies different entities within the
            collection. This is meant to replace the `join_key` parameter, but currently only
            supports a list of size one.
    """

    name: str
    value_type: ValueType
    join_key: str
    description: str
    tags: Dict[str, str]
    owner: str
    created_timestamp: Optional[datetime]
    last_updated_timestamp: Optional[datetime]
    join_keys: List[str]

    @log_exceptions
    def __init__(
        self,
        *args,
        name: Optional[str] = None,
        value_type: Optional[ValueType] = None,
        description: str = "",
        join_key: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
        join_keys: Optional[List[str]] = None,
    ):
        """
        Creates an Entity object.

        Args:
            name: The unique name of the entity.
            value_type (deprecated): The type of the entity, such as string or float.
            description: A human-readable description.
            join_key (deprecated): A property that uniquely identifies different entities within the
                collection. The join_key property is typically used for joining entities
                with their associated features. If not specified, defaults to the name.
            tags: A dictionary of key-value pairs to store arbitrary metadata.
            owner: The owner of the entity, typically the email of the primary maintainer.
            join_keys: A list of properties that uniquely identifies different entities within the
                collection. This is meant to replace the `join_key` parameter, but currently only
                supports a list of size one.

        Raises:
            ValueError: Parameters are specified incorrectly.
        """
        if len(args) == 1:
            warnings.warn(
                (
                    "Entity name should be specified as a keyword argument instead of a positional arg."
                    "Feast 0.24+ will not support positional arguments to construct Entities"
                ),
                DeprecationWarning,
            )
        if len(args) > 1:
            raise ValueError(
                "All arguments to construct an entity should be specified as keyword arguments only"
            )

        self.name = args[0] if len(args) > 0 else name

        if not self.name:
            raise ValueError("Name needs to be specified")

        if value_type:
            warnings.warn(
                (
                    "The `value_type` parameter is being deprecated. Instead, the type of an entity "
                    "should be specified as a Field in the schema of a feature view. Feast 0.24 and "
                    "onwards will not support the `value_type` parameter. The `entities` parameter of "
                    "feature views should also be changed to a List[Entity] instead of a List[str]; if "
                    "this is not done, entity columns will be mistakenly interpreted as feature columns."
                ),
                DeprecationWarning,
            )
        self.value_type = value_type or ValueType.UNKNOWN

        # For now, both the `join_key` and `join_keys` attributes are set correctly,
        # so both are usable.
        # TODO(felixwang9817): Remove the usage of `join_key` throughout the codebase
        # when the usage of `join_key` as a parameter is removed.
        if join_key:
            warnings.warn(
                (
                    "The `join_key` parameter is being deprecated in favor of the `join_keys` parameter. "
                    "Please switch from using `join_key` to `join_keys`. Feast 0.24 and onwards will not "
                    "support the `join_key` parameter."
                ),
                DeprecationWarning,
            )
        self.join_keys = join_keys or []
        if join_keys and len(join_keys) > 1:
            raise ValueError(
                "An entity may only have single join key. "
                "Multiple join keys will be supported in the future."
            )
        if join_keys and len(join_keys) == 1:
            self.join_key = join_keys[0]
        else:
            self.join_key = join_key if join_key else self.name
        if not self.join_keys:
            self.join_keys = [self.join_key]
        self.description = description
        self.tags = tags if tags is not None else {}
        self.owner = owner
        self.created_timestamp = None
        self.last_updated_timestamp = None

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
