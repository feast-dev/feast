# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
import typing

from pydantic import BaseModel, ConfigDict, Field

from .Value_p2p import ValueType


class Field(BaseModel):
    model_config = ConfigDict(validate_default=True)
    name: str = Field(default="")
    value: ValueType.Enum = Field(default=0)
    # Tags for user defined metadata on a field
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    # Description of the field.
    description: str = Field(default="")
