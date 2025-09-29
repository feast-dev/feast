# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from .Value_p2p import Enum
from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
import typing


class Field(BaseModel):
    model_config = ConfigDict(validate_default=True)
    name: str = Field(default="")
    value: Enum = Field(default=0)
# Tags for user defined metadata on a field
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
# Description of the field.
    description: str = Field(default="")
