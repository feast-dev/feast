# Copyright 2021 The Feast Authors
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
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List

from feast.errors import FeastInvalidInfraObjectType
from feast.importer import import_class
from feast.protos.feast.core.DatastoreTable_pb2 import (
    DatastoreTable as DatastoreTableProto,
)
from feast.protos.feast.core.InfraObject_pb2 import Infra as InfraProto
from feast.protos.feast.core.InfraObject_pb2 import InfraObject as InfraObjectProto
from feast.protos.feast.core.SqliteTable_pb2 import SqliteTable as SqliteTableProto

DATASTORE_INFRA_OBJECT_CLASS_TYPE = "feast.infra.online_stores.datastore.DatastoreTable"
SQLITE_INFRA_OBJECT_CLASS_TYPE = "feast.infra.online_stores.sqlite.SqliteTable"


class InfraObject(ABC):
    """
    Represents a single infrastructure object (e.g. online store table) managed by Feast.
    """

    @abstractmethod
    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @abstractmethod
    def to_infra_object_proto(self) -> InfraObjectProto:
        """Converts an InfraObject to its protobuf representation, wrapped in an InfraObjectProto."""
        pass

    @abstractmethod
    def to_proto(self) -> Any:
        """Converts an InfraObject to its protobuf representation."""
        pass

    def __lt__(self, other) -> bool:
        return self.name < other.name

    @staticmethod
    @abstractmethod
    def from_infra_object_proto(infra_object_proto: InfraObjectProto) -> Any:
        """
        Returns an InfraObject created from a protobuf representation.

        Args:
            infra_object_proto: A protobuf representation of an InfraObject.

        Raises:
            FeastInvalidInfraObjectType: The type of InfraObject could not be identified.
        """
        if infra_object_proto.infra_object_class_type:
            cls = _get_infra_object_class_from_type(
                infra_object_proto.infra_object_class_type
            )
            return cls.from_infra_object_proto(infra_object_proto)

        raise FeastInvalidInfraObjectType()

    @staticmethod
    def from_proto(infra_object_proto: Any) -> Any:
        """
        Converts a protobuf representation of a subclass to an object of that subclass.

        Args:
            infra_object_proto: A protobuf representation of an InfraObject.

        Raises:
            FeastInvalidInfraObjectType: The type of InfraObject could not be identified.
        """
        if isinstance(infra_object_proto, DatastoreTableProto):
            infra_object_class_type = DATASTORE_INFRA_OBJECT_CLASS_TYPE
        elif isinstance(infra_object_proto, SqliteTableProto):
            infra_object_class_type = SQLITE_INFRA_OBJECT_CLASS_TYPE
        else:
            raise FeastInvalidInfraObjectType()

        cls = _get_infra_object_class_from_type(infra_object_class_type)
        return cls.from_proto(infra_object_proto)

    @abstractmethod
    def update(self):
        """
        Deploys or updates the infrastructure object.
        """
        pass

    @abstractmethod
    def teardown(self):
        """
        Tears down the infrastructure object.
        """
        pass


@dataclass
class Infra:
    """
    Represents the set of infrastructure managed by Feast.

    Args:
        infra_objects: A list of InfraObjects, each representing one infrastructure object.
    """

    infra_objects: List[InfraObject] = field(default_factory=list)

    def to_proto(self) -> InfraProto:
        """
        Converts Infra to its protobuf representation.

        Returns:
            An InfraProto protobuf.
        """
        infra_proto = InfraProto()
        for infra_object in self.infra_objects:
            infra_object_proto = infra_object.to_infra_object_proto()
            infra_proto.infra_objects.append(infra_object_proto)

        return infra_proto

    @classmethod
    def from_proto(cls, infra_proto: InfraProto):
        """
        Returns an Infra object created from a protobuf representation.
        """
        infra = cls()
        infra.infra_objects += [
            InfraObject.from_infra_object_proto(infra_object_proto)
            for infra_object_proto in infra_proto.infra_objects
        ]

        return infra


def _get_infra_object_class_from_type(infra_object_class_type: str):
    module_name, infra_object_class_name = infra_object_class_type.rsplit(".", 1)
    return import_class(module_name, infra_object_class_name)
