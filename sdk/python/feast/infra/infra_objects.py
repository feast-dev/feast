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
from typing import List

from feast.protos.feast.core.InfraObjects_pb2 import Infra as InfraProto


class InfraObject(ABC):
    """
    Represents a single infrastructure object (e.g. online store table) managed by Feast.
    """

    @abstractmethod
    def to_proto(self):
        """Converts an InfraObject to its protobuf representation."""
        pass

    @classmethod
    @abstractmethod
    def from_proto(cls, infra_object_proto):
        """
        Returns an InfraObject created from a protobuf representation.

        Args:
            infra_object_proto: A protobuf representation of an InfraObject.
        """
        pass

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
        objects: A list of InfraObjects, each representing one infrastructure object.
    """

    objects: List[InfraObject] = field(default_factory=list)

    def to_proto(self) -> InfraProto:
        """
        Converts Infra to its protobuf representation.

        Returns:
            An InfraProto protobuf.
        """
