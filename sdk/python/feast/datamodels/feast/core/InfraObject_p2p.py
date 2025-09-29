# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
import typing

from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel, Field, model_validator

from .DatastoreTable_p2p import DatastoreTable
from .SqliteTable_p2p import SqliteTable


class InfraObject(BaseModel):
    """
    Represents a single infrastructure object managed by Feast
    """

    class CustomInfra(BaseModel):
        """
        Allows for custom infra objects to be added
        """

        field: bytes = Field(default=b"")

    _one_of_dict = {
        "InfraObject.infra_object": {
            "fields": {"custom_infra", "datastore_table", "sqlite_table"}
        }
    }
    one_of_validator = model_validator(mode="before")(check_one_of)
    # Represents the Python class for the infrastructure object
    infra_object_class_type: str = Field(default="")
    datastore_table: DatastoreTable = Field(default_factory=DatastoreTable)
    sqlite_table: SqliteTable = Field(default_factory=SqliteTable)
    custom_infra: CustomInfra = Field(default_factory=CustomInfra)


class Infra(BaseModel):
    """
    Represents a set of infrastructure objects managed by Feast
    """

    # List of infrastructure objects managed by Feast
    infra_objects: typing.List[InfraObject] = Field(default_factory=list)
