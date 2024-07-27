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
import uuid
from typing import Optional

from google.protobuf.json_format import MessageToJson
from typeguard import typechecked

from feast.permissions.decision import DecisionStrategy
from feast.protos.feast.core.Registry_pb2 import ProjectMetadata as ProjectMetadataProto


@typechecked
class ProjectMetadata:
    """
    Tracks project level metadata

    Attributes:
        project_name: The registry-scoped unique name of the project.
        project_uuid: The UUID for this project
    """

    project_name: str
    project_uuid: str
    decision_strategy: DecisionStrategy

    def __init__(
        self,
        *args,
        project_name: Optional[str] = None,
        project_uuid: Optional[str] = None,
        decision_strategy: Optional[DecisionStrategy] = None,
    ):
        """
        Creates an Project metadata object.

        Args:
            project_name: The registry-scoped unique name of the project.
            project_uuid: The UUID for this project
            decision_strategy: The projects decision strategy to be applied when multiple permissions match an execution request.

        Raises:
            ValueError: Parameters are specified incorrectly.
        """
        if not project_name:
            raise ValueError("Project name needs to be specified")

        self.project_name = project_name
        self.project_uuid = project_uuid or f"{uuid.uuid4()}"
        self.decision_strategy = decision_strategy or DecisionStrategy.UNANIMOUS

    def __hash__(self) -> int:
        return hash((self.project_name, self.project_uuid, self.decision_strategy))

    def __eq__(self, other):
        if not isinstance(other, ProjectMetadata):
            raise TypeError(
                "Comparisons should only involve ProjectMetadata class objects."
            )

        if (
            self.project_name != other.project_name
            or self.project_uuid != other.project_uuid
            or self.decision_strategy != other.decision_strategy
        ):
            return False

        return True

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __lt__(self, other):
        return self.project_name < other.project_name

    @classmethod
    def from_proto(cls, project_metadata_proto: ProjectMetadataProto):
        """
        Creates project metadata from a protobuf representation.

        Args:
            project_metadata_proto: A protobuf representation of project metadata.

        Returns:
            A ProjectMetadata object based on the protobuf.
        """
        entity = cls(
            project_name=project_metadata_proto.project,
            project_uuid=project_metadata_proto.project_uuid,
            decision_strategy=DecisionStrategy[
                ProjectMetadataProto.DecisionStrategy.Name(
                    project_metadata_proto.decision_strategy
                )
            ],
        )

        return entity

    def to_proto(self) -> ProjectMetadataProto:
        """
        Converts a project metadata object to its protobuf representation.

        Returns:
            An ProjectMetadataProto protobuf.
        """

        return ProjectMetadataProto(
            project=self.project_name,
            project_uuid=self.project_uuid,
            decision_strategy=ProjectMetadataProto.DecisionStrategy.Value(
                self.decision_strategy.name
            ),
        )
