# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from datetime import timedelta
from google.protobuf.message import Message  # type: ignore
from protobuf_to_pydantic.util import Timedelta
from pydantic import BaseModel
from pydantic import BeforeValidator
from pydantic import Field
from typing_extensions import Annotated


class Aggregation(BaseModel):
    column: str = Field(default="")
    function: str = Field(default="")
    time_window: Annotated[timedelta, BeforeValidator(Timedelta.validate)] = Field(default_factory=timedelta)
    slide_interval: Annotated[timedelta, BeforeValidator(Timedelta.validate)] = Field(default_factory=timedelta)
