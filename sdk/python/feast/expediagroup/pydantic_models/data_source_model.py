"""
Pydantic Model for Data Source

Copyright 2023 Expedia Group
Author: matcarlin@expediagroup.com
"""
from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel
from pydantic import Field as PydanticField
from typing_extensions import Annotated

from feast.data_source import RequestSource
from feast.field import Field
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)


class DataSourceModel(BaseModel):
    """
    Pydantic Model of a Feast DataSource.
    """

    def to_data_source(self):
        """
        Given a Pydantic DataSourceModel, create and return a DataSource.

        Returns:
            A DataSource.
        """
        raise NotImplementedError

    @classmethod
    def from_data_source(cls, data_source):
        """
        Converts a DataSource object to its pydantic model representation.

        Returns:
            A DataSourceModel.
        """
        raise NotImplementedError


class RequestSourceModel(DataSourceModel):
    """
    Pydantic Model of a Feast RequestSource.
    """

    name: str
    model_type: Literal["RequestSourceModel"] = "RequestSourceModel"
    schema_: List[Field] = PydanticField(None, alias="schema")
    description: Optional[str] = ""
    tags: Optional[Dict[str, str]] = None
    owner: Optional[str] = ""

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"

    def to_data_source(self):
        """
        Given a Pydantic RequestSourceModel, create and return a RequestSource.

        Returns:
            A RequestSource.
        """
        params = {
            "name": self.name,
            "description": self.description,
            "tags": self.tags if self.tags else None,
            "owner": self.owner,
        }
        params["schema"] = [
            Field(
                name=sch.name,
                dtype=sch.dtype,
                description=sch.description,
                tags=sch.tags,
            )
            for sch in self.schema_
        ]
        return RequestSource(**params)

    @classmethod
    def from_data_source(cls, data_source):
        """
        Converts a RequestSource object to its pydantic model representation.

        Returns:
            A RequestSourceModel.
        """
        return cls(
            name=data_source.name,
            schema=data_source.schema,
            description=data_source.description,
            tags=data_source.tags if data_source.tags else None,
            owner=data_source.owner,
        )


class SparkSourceModel(DataSourceModel):
    """
    Pydantic Model of a Feast SparkSource.
    """

    name: str
    model_type: Literal["SparkSourceModel"] = "SparkSourceModel"
    table: Optional[str] = None
    query: Optional[str] = None
    path: Optional[str] = None
    file_format: Optional[str] = None
    created_timestamp_column: Optional[str] = None
    field_mapping: Optional[Dict[str, str]] = None
    description: Optional[str] = ""
    tags: Optional[Dict[str, str]] = None
    owner: Optional[str] = ""
    timestamp_field: Optional[str] = None

    class Config:
        arbitrary_types_allowed = True
        extra = "allow"

    def to_data_source(self):
        """
        Given a Pydantic SparkSourceModel, create and return a SparkSource.

        Returns:
            A SparkSource.
        """
        return SparkSource(
            name=self.name,
            table=self.table if hasattr(self, "table") else "",
            query=self.query if hasattr(self, "query") else "",
            path=self.path if hasattr(self, "path") else "",
            file_format=self.file_format if hasattr(self, "file_format") else "",
            created_timestamp_column=self.created_timestamp_column
            if hasattr(self, "created_timestamp_column")
            else "",
            field_mapping=self.field_mapping if self.field_mapping else None,
            description=self.description or "",
            tags=self.tags if self.tags else None,
            owner=self.owner or "",
            timestamp_field=self.timestamp_field
            if hasattr(self, "timestamp_field")
            else "",
        )

    @classmethod
    def from_data_source(cls, data_source):
        """
        Converts a SparkSource object to its pydantic model representation.

        Returns:
            A SparkSourceModel.
        """
        return cls(
            name=data_source.name,
            table=data_source.table,
            query=data_source.query,
            path=data_source.path,
            file_format=data_source.file_format,
            created_timestamp_column=data_source.created_timestamp_column
            if data_source.created_timestamp_column
            else "",
            field_mapping=data_source.field_mapping
            if data_source.field_mapping
            else None,
            description=data_source.description if data_source.description else "",
            tags=data_source.tags if data_source.tags else None,
            owner=data_source.owner if data_source.owner else "",
            timestamp_field=data_source.timestamp_field
            if data_source.timestamp_field
            else "",
        )


# https://blog.devgenius.io/deserialize-child-classes-with-pydantic-that-gonna-work-784230e1cf83
# This lets us discriminate child classes of DataSourceModel with type hints.
AnyDataSource = Annotated[
    Union[RequestSourceModel, SparkSourceModel],
    PydanticField(discriminator="model_type"),
]
