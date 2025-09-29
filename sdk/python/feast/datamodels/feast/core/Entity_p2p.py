# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from ..types.Value_p2p import Enum
from datetime import datetime
from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
import typing


class EntitySpecV2(BaseModel):
    model_config = ConfigDict(validate_default=True)
# Name of the entity.
    name: str = Field(default="")
# Name of Feast project that this feature table belongs to.
    project: str = Field(default="")
# Type of the entity.
    value_type: Enum = Field(default=0)
# Description of the entity.
    description: str = Field(default="")
# Join key for the entity (i.e. name of the column the entity maps to).
    join_key: str = Field(default="")
# User defined metadata
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
# Owner of the entity.
    owner: str = Field(default="")

class EntityMeta(BaseModel):
    created_timestamp: datetime = Field(default_factory=datetime.now)
    last_updated_timestamp: datetime = Field(default_factory=datetime.now)

class Entity(BaseModel):
# User-specified specifications of this entity.
    spec: EntitySpecV2 = Field(default_factory=EntitySpecV2)
# System-populated metadata for this entity.
    meta: EntityMeta = Field(default_factory=EntityMeta)

class EntityList(BaseModel):
    entities: typing.List[Entity] = Field(default_factory=list)
