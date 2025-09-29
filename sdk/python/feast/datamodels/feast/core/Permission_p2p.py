# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from .Policy_p2p import Policy
from datetime import datetime
from enum import IntEnum
from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
import typing


class PermissionSpec(BaseModel):
    class AuthzedAction(IntEnum):
        CREATE = 0
        DESCRIBE = 1
        UPDATE = 2
        DELETE = 3
        READ_ONLINE = 4
        READ_OFFLINE = 5
        WRITE_ONLINE = 6
        WRITE_OFFLINE = 7

    class Type(IntEnum):
        FEATURE_VIEW = 0
        ON_DEMAND_FEATURE_VIEW = 1
        BATCH_FEATURE_VIEW = 2
        STREAM_FEATURE_VIEW = 3
        ENTITY = 4
        FEATURE_SERVICE = 5
        DATA_SOURCE = 6
        VALIDATION_REFERENCE = 7
        SAVED_DATASET = 8
        PERMISSION = 9
        PROJECT = 10

    model_config = ConfigDict(validate_default=True)
# Name of the permission. Must be unique. Not updated.
    name: str = Field(default="")
# Name of Feast project.
    project: str = Field(default="")
    types: typing.List[Type] = Field(default_factory=list)
    name_patterns: typing.List[str] = Field(default_factory=list)
    required_tags: "typing.Dict[str, str]" = Field(default_factory=dict)
# List of actions.
    actions: typing.List[AuthzedAction] = Field(default_factory=list)
# the policy.
    policy: Policy = Field(default_factory=Policy)
# User defined metadata
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)

class PermissionMeta(BaseModel):
    created_timestamp: datetime = Field(default_factory=datetime.now)
    last_updated_timestamp: datetime = Field(default_factory=datetime.now)

class Permission(BaseModel):
# User-specified specifications of this permission.
    spec: PermissionSpec = Field(default_factory=PermissionSpec)
# System-populated metadata for this permission.
    meta: PermissionMeta = Field(default_factory=PermissionMeta)
