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
from datetime import datetime, timezone
from typing import Dict, Optional

from google.protobuf.json_format import MessageToJson
from typeguard import typechecked

from feast.protos.feast.core.Project_pb2 import Project as ProjectProto
from feast.protos.feast.core.Project_pb2 import ProjectMeta as ProjectMetaProto
from feast.protos.feast.core.Project_pb2 import ProjectSpec as ProjectSpecProto
from feast.utils import _utc_now


@typechecked
class Project:
    """
    Project is a collection of Feast Objects. Projects provide complete isolation of
    feature stores at the infrastructure level.

    Attributes:
        name: The unique name of the project.
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the project, typically the email of the primary maintainer.
        created_timestamp: The time when the entity was created.
        last_updated_timestamp: The time when the entity was last updated.
    """

    name: str
    description: str
    tags: Dict[str, str]
    owner: str
    created_timestamp: datetime
    last_updated_timestamp: datetime

    def __init__(
        self,
        *,
        name: str,
        description: str = "",
        tags: Optional[Dict[str, str]] = None,
        owner: str = "",
        created_timestamp: Optional[datetime] = None,
        last_updated_timestamp: Optional[datetime] = None,
    ):
        """
        Creates Project object.

        Args:
            name: The unique name of the project.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the project, typically the email of the primary maintainer.
            created_timestamp (optional): The time when the project was created. Defaults to
            last_updated_timestamp (optional): The time when the project was last updated.

        Raises:
            ValueError: Parameters are specified incorrectly.
        """
        self.name = name
        self.description = description
        self.tags = tags if tags is not None else {}
        self.owner = owner
        updated_time = _utc_now()
        self.created_timestamp = created_timestamp or updated_time
        self.last_updated_timestamp = last_updated_timestamp or updated_time

    def __hash__(self) -> int:
        return hash((self.name))

    def __eq__(self, other):
        if not isinstance(other, Project):
            raise TypeError("Comparisons should only involve Project class objects.")

        if (
            self.name != other.name
            or self.description != other.description
            or self.tags != other.tags
            or self.owner != other.owner
            or self.created_timestamp != other.created_timestamp
            or self.last_updated_timestamp != other.last_updated_timestamp
        ):
            return False

        return True

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __lt__(self, other):
        return self.name < other.name

    def is_valid(self):
        """
        Validates the state of this project locally.

        Raises:
            ValueError: The project does not have a name or does not have a type.
        """
        if not self.name:
            raise ValueError("The project does not have a name.")

        from feast.repo_operations import is_valid_name

        if not is_valid_name(self.name):
            raise ValueError(
                f"Project name, {self.name}, should only have "
                f"alphanumerical values, underscores, and hyphens but not start with an underscore or hyphen."
            )

    @classmethod
    def from_proto(cls, project_proto: ProjectProto):
        """
        Creates a project from a protobuf representation of an project.

        Args:
            entity_proto: A protobuf representation of an project.

        Returns:
            An Entity object based on the entity protobuf.
        """
        project = cls(
            name=project_proto.spec.name,
            description=project_proto.spec.description,
            tags=dict(project_proto.spec.tags),
            owner=project_proto.spec.owner,
        )
        if project_proto.meta.HasField("created_timestamp"):
            project.created_timestamp = (
                project_proto.meta.created_timestamp.ToDatetime().replace(
                    tzinfo=timezone.utc
                )
            )
        if project_proto.meta.HasField("last_updated_timestamp"):
            project.last_updated_timestamp = (
                project_proto.meta.last_updated_timestamp.ToDatetime().replace(
                    tzinfo=timezone.utc
                )
            )

        return project

    def to_proto(self) -> ProjectProto:
        """
        Converts an project object to its protobuf representation.

        Returns:
            An ProjectProto protobuf.
        """
        meta = ProjectMetaProto()
        if self.created_timestamp:
            meta.created_timestamp.FromDatetime(self.created_timestamp)
        if self.last_updated_timestamp:
            meta.last_updated_timestamp.FromDatetime(self.last_updated_timestamp)

        spec = ProjectSpecProto(
            name=self.name,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        return ProjectProto(spec=spec, meta=meta)
