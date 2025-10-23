# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
from pydantic import BaseModel, Field


class SqliteTable(BaseModel):
    """
    Represents a Sqlite table
    """

    # Absolute path of the table
    path: str = Field(default="")
    # Name of the table
    name: str = Field(default="")
