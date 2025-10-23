# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
import typing
from datetime import datetime

from pydantic import BaseModel, Field


class ProjectSpec(BaseModel):
    # Name of the Project
    name: str = Field(default="")
    # Description of the Project
    description: str = Field(default="")
    # User defined metadata
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    # Owner of the Project
    owner: str = Field(default="")


class ProjectMeta(BaseModel):
    # Time when the Project is created
    created_timestamp: datetime = Field(default_factory=datetime.now)
    # Time when the Project is last updated with registry changes (Apply stage)
    last_updated_timestamp: datetime = Field(default_factory=datetime.now)


class Project(BaseModel):
    # User-specified specifications of this entity.
    spec: ProjectSpec = Field(default_factory=ProjectSpec)
    # System-populated metadata for this entity.
    meta: ProjectMeta = Field(default_factory=ProjectMeta)
