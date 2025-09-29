# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from google.protobuf.message import Message  # type: ignore
from google.protobuf.wrappers_pb2 import StringValue  # type: ignore
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field


class DatastoreTable(BaseModel):
    """
     Represents a Datastore table
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)
# Feast project of the table
    project: str = Field(default="")
# Name of the table
    name: str = Field(default="")
# GCP project id
    project_id: StringValue = Field(default_factory=StringValue)
# Datastore namespace
    namespace: StringValue = Field(default_factory=StringValue)
# Firestore database
    database: StringValue = Field(default_factory=StringValue)
