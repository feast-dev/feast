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

from collections import OrderedDict
from typing import Dict, Mapping, MutableMapping, Optional

import yaml
from google.protobuf import json_format
from google.protobuf.json_format import MessageToDict, MessageToJson
from google.protobuf.timestamp_pb2 import Timestamp

from feast.core.Entity_pb2 import Entity as EntityV2Proto
from feast.core.Entity_pb2 import EntityMeta as EntityMetaProto
from feast.core.Entity_pb2 import EntitySpecV2 as EntitySpecProto
from feast.core.FeatureSet_pb2 import EntitySpec as EntityProto
from feast.field import Field
from feast.loaders import yaml as feast_yaml
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
        return EntityProto(name=self.name, value_type=value_type,)

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
        return entity


class EntityV2:
    """
    Represents a collection of entities and associated metadata.
    """

    def __init__(
        self,
        name: str,
        description: str,
        columns: Mapping[str, ValueType],
        labels: Optional[MutableMapping[str, str]] = None,
    ):
        self._name = name
        self._description = description
        self._columns = columns
        if labels is None:
            self._labels = OrderedDict()  # type: MutableMapping[str, str]
        else:
            self._labels = labels

        self._created_timestamp: Optional[Timestamp] = None

    def __eq__(self, other):
        if not isinstance(other, EntityV2):
            return NotImplemented

        self.columns = {
            k: (v.value if isinstance(v, ValueType) else v)
            for k, v in self.columns.items()
        }
        other.columns = {
            k: (v.value if isinstance(v, ValueType) else v)
            for k, v in other.columns.items()
        }
        if (
            self.labels != other.labels
            or self.name != other.name
            or self.description != other.description
            or self.columns != other.columns
        ):
            return False

        return True

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    @property
    def name(self):
        """
        Returns the name of this entity
        """
        return self._name

    @name.setter
    def name(self, name):
        """
        Sets the name of this entity
        """
        self._name = name

    @property
    def description(self):
        """
        Returns the description of this entity
        """
        return self._description

    @description.setter
    def description(self, description):
        """
        Sets the description of this entity
        """
        self._description = description

    @property
    def columns(self):
        """
        Returns the columns of this entity
        """
        return self._columns

    @columns.setter
    def columns(self, columns: Mapping[str, ValueType]):
        """
        Set the columns for this entity
        """
        self._columns = columns

    @property
    def labels(self):
        """
        Returns the labels of this entity. This is the user defined metadata
        defined as a dictionary.
        """
        return self._labels

    @labels.setter
    def labels(self, labels: MutableMapping[str, str]):
        """
        Set the labels for this entity
        """
        self._labels = labels

    @property
    def created_timestamp(self):
        """
        Returns the created_timestamp of this entity
        """
        return self._created_timestamp

    @created_timestamp.setter
    def created_timestamp(self, created_timestamp):
        """
        Sets the status of this entity
        """
        self._created_timestamp = created_timestamp

    def is_valid(self):
        """
        Validates the state of a entity locally. Raises an exception
        if entity is invalid.
        """

        if not self.name:
            raise ValueError("No name found in entity.")

        if len(self.columns) == 0:
            raise ValueError("No columns found in entity {self.name}")

    @classmethod
    def from_yaml(cls, yml: str):
        """
        Creates an entity from a YAML string body or a file path

        Args:
            yml: Either a file path containing a yaml file or a YAML string

        Returns:
            Returns a EntityV2 object based on the YAML file
        """

        return cls.from_dict(feast_yaml.yaml_loader(yml, load_single=True))

    @classmethod
    def from_dict(cls, entity_dict):
        """
        Creates an entity from a dict

        Args:
            entity_dict: A dict representation of an entity

        Returns:
            Returns a EntityV2 object based on the entity dict
        """

        entity_proto = json_format.ParseDict(
            entity_dict, EntityV2Proto(), ignore_unknown_fields=True
        )
        return cls.from_proto(entity_proto)

    @classmethod
    def from_proto(cls, entity_proto: EntityV2Proto):
        """
        Creates an entity from a protobuf representation of an entity

        Args:
            entity_proto: A protobuf representation of an entity

        Returns:
            Returns a EntityV2 object based on the entity protobuf
        """

        entity = cls(
            name=entity_proto.spec.name,
            description=entity_proto.spec.description,
            columns=entity_proto.spec.columns,  # type: ignore
            labels=entity_proto.spec.labels,
        )

        entity._created_timestamp = entity_proto.meta.created_timestamp

        return entity

    def to_proto(self) -> EntityV2Proto:
        """
        Converts an entity object to its protobuf representation

        Returns:
            EntityV2Proto protobuf
        """

        meta = EntityMetaProto(created_timestamp=self.created_timestamp)

        spec = EntitySpecProto(
            name=self.name,
            description=self.description,
            columns={
                k: (v.value if isinstance(v, ValueType) else v)
                for k, v in self.columns.items()
            },
            labels=self.labels,
        )

        return EntityV2Proto(spec=spec, meta=meta)

    def to_dict(self) -> Dict:
        """
        Converts entity to dict

        Returns:
            Dictionary object representation of entity
        """

        entity_dict = MessageToDict(self.to_proto())

        # Remove meta when empty for more readable exports
        if entity_dict["meta"] == {}:
            del entity_dict["meta"]

        return entity_dict

    def to_yaml(self):
        """
        Converts a entity to a YAML string.

        Returns:
            Entity string returned in YAML format
        """
        entity_dict = self.to_dict()
        return yaml.dump(entity_dict, allow_unicode=True, sort_keys=False)

    def _update_from_entity(self, entity):
        """
        Deep replaces one entity with another

        Args:
            entity: Entity to use as a source of configuration
        """

        self.name = entity.name
        self.description = entity.description
        self.columns = entity.columns
        self.labels = entity.labels
        self.created_timestamp = entity.created_timestamp
