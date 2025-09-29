# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from enum import IntEnum
from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

class ServingStatus(IntEnum):
    UNKNOWN = 0
    SERVING = 1
    NOT_SERVING = 2

class HealthCheckRequest(BaseModel):
    service: str = Field(default="")

class HealthCheckResponse(BaseModel):
    model_config = ConfigDict(validate_default=True)
    status: ServingStatus = Field(default=0)
