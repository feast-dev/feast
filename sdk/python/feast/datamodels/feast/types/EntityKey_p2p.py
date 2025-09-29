# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from .Value_p2p import Value
from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel
from pydantic import Field
import typing


class EntityKey(BaseModel):
    join_keys: typing.List[str] = Field(default_factory=list)
    entity_values: typing.List[Value] = Field(default_factory=list)
