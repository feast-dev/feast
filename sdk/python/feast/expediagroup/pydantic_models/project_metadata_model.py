from datetime import datetime

from pydantic import BaseModel
from typing_extensions import Self

from feast.project_metadata import ProjectMetadata


class ProjectMetadataModel(BaseModel):
    """
    Pydantic Model of a Feast Field.
    """

    project_name: str
    project_uuid: str = ""
    last_updated_timestamp: datetime = datetime.utcfromtimestamp(1)

    def to_project_metadata(self) -> ProjectMetadata:
        """
        Given a Pydantic ProjectMetadataModel, create and return a ProjectMetadata.

        Returns:
            A ProjectMetadata.
        """
        return ProjectMetadata(
            project_name=self.project_name,
            project_uuid=self.project_uuid,
            last_updated_timestamp=self.last_updated_timestamp,
        )

    @classmethod
    def from_project_metadata(
        cls,
        project_metadata: ProjectMetadata,
    ) -> Self:  # type: ignore
        """
        Converts a ProjectMetadata object to its pydantic ProjectMetadataModel representation.

        Returns:
            A ProjectMetadataModel.
        """
        return cls(
            project_name=project_metadata.project_name,
            project_uuid=project_metadata.project_uuid,
            last_updated_timestamp=project_metadata.last_updated_timestamp,
        )
