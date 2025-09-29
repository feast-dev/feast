# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
import typing

from pydantic import BaseModel, Field


class PushRequest(BaseModel):
    features: "typing.Dict[str, str]" = Field(default_factory=dict)
    stream_feature_view: str = Field(default="")
    allow_registry_cache: bool = Field(default=False)
    to: str = Field(default="")


class PushResponse(BaseModel):
    status: bool = Field(default=False)


class WriteToOnlineStoreRequest(BaseModel):
    features: "typing.Dict[str, str]" = Field(default_factory=dict)
    feature_view_name: str = Field(default="")
    allow_registry_cache: bool = Field(default=False)


class WriteToOnlineStoreResponse(BaseModel):
    status: bool = Field(default=False)
