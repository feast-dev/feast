"""
Pydantic Model for Data Source

Copyright 2023 Expedia Group
Author: matcarlin@expediagroup.com
"""

import sys
from datetime import datetime, timedelta
from typing import Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, field_serializer, field_validator
from pydantic import Field as PydanticField
from typing_extensions import Annotated, Self

from feast.data_source import KafkaSource, PushSource, RequestSource
from feast.expediagroup.pydantic_models.field_model import FieldModel
from feast.expediagroup.pydantic_models.stream_format_model import (
    AnyStreamFormat,
    AvroFormatModel,
    ConfluentAvroFormatModel,
    JsonFormatModel,
    ProtoFormatModel,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)


class DataSourceModel(BaseModel):
    """
    Pydantic Model of a Feast DataSource.
    """

    created_timestamp: Optional[datetime] = None
    last_updated_timestamp: Optional[datetime] = None

    def to_data_source(self):
        """
        Given a Pydantic DataSourceModel, create and return a DataSource.

        Returns:
            A DataSource.
        """
        raise NotImplementedError

    def _attach_timestamps(self, data_source):
        data_source.created_timestamp = self.created_timestamp
        data_source.last_updated_timestamp = self.last_updated_timestamp
        return data_source

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

    model_config = ConfigDict(protected_namespaces=())

    name: str
    model_type: Literal["RequestSourceModel"] = "RequestSourceModel"
    schema_: List[FieldModel]
    description: Optional[str] = ""
    tags: Optional[Dict[str, str]] = None
    owner: Optional[str] = ""

    def to_data_source(self) -> RequestSource:
        """
        Given a Pydantic RequestSourceModel, create and return a RequestSource.

        Returns:
            A RequestSource.
        """
        source = RequestSource(
            name=self.name,
            schema=[sch.to_field() for sch in self.schema_],
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )
        return self._attach_timestamps(source)

    @classmethod
    def from_data_source(
        cls,
        data_source,
    ) -> Self:  # type: ignore
        """
        Converts a RequestSource object to its pydantic model representation.

        Returns:
            A RequestSourceModel.
        """
        return cls(
            name=data_source.name,
            schema_=[
                FieldModel.from_field(ds_schema) for ds_schema in data_source.schema
            ],
            description=data_source.description,
            tags=data_source.tags if data_source.tags else None,
            owner=data_source.owner,
            created_timestamp=data_source.created_timestamp,
            last_updated_timestamp=data_source.last_updated_timestamp,
        )


class SparkSourceModel(DataSourceModel):
    """
    Pydantic Model of a Feast SparkSource.
    """

    model_config = ConfigDict(protected_namespaces=())

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

    def to_data_source(self) -> SparkSource:
        """
        Given a Pydantic SparkSourceModel, create and return a SparkSource.

        Returns:
            A SparkSource.
        """
        source = SparkSource(
            name=self.name,
            table=self.table,
            query=self.query,
            path=self.path,
            file_format=self.file_format,
            created_timestamp_column=self.created_timestamp_column,
            field_mapping=self.field_mapping,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            timestamp_field=self.timestamp_field,
        )
        return self._attach_timestamps(source)

    @classmethod
    def from_data_source(
        cls,
        data_source,
    ) -> Self:  # type: ignore
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
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=data_source.field_mapping,
            description=data_source.description,
            tags=data_source.tags,
            owner=data_source.owner,
            timestamp_field=data_source.timestamp_field,
            created_timestamp=data_source.created_timestamp,
            last_updated_timestamp=data_source.last_updated_timestamp,
        )


AnyBatchDataSource = Annotated[
    Union[RequestSourceModel, SparkSourceModel],
    PydanticField(discriminator="model_type"),
]


SUPPORTED_PUSH_BATCH_SOURCES = [SparkSourceModel]


class PushSourceModel(DataSourceModel):
    """
    Pydantic Model of a Feast PushSource.
    """

    model_config = ConfigDict(protected_namespaces=())

    name: str
    model_type: Literal["PushSourceModel"] = "PushSourceModel"
    batch_source: AnyBatchDataSource
    description: Optional[str] = ""
    tags: Optional[Dict[str, str]] = None
    owner: Optional[str] = ""

    def to_data_source(self) -> PushSource:
        """
        Given a Pydantic PushSourceModel, create and return a PushSource.

        Returns:
            A SparkSource.
        """
        source = PushSource(
            name=self.name,
            batch_source=self.batch_source.to_data_source(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )
        return self._attach_timestamps(source)

    @classmethod
    def from_data_source(
        cls,
        data_source,
    ) -> Self:  # type: ignore
        """
        Converts a PushSource object to its pydantic model representation.

        Returns:
            A PushSourceModel.
        """
        class_ = getattr(
            sys.modules[__name__],
            type(data_source.batch_source).__name__ + "Model",
        )
        if class_ not in SUPPORTED_PUSH_BATCH_SOURCES:
            raise ValueError(
                "Push Source's batch source type is not a supported data source type."
            )
        batch_source = class_.from_data_source(data_source.batch_source)

        return cls(
            name=data_source.name,
            batch_source=batch_source,
            description=data_source.description,
            tags=data_source.tags,
            owner=data_source.owner,
            created_timestamp=data_source.created_timestamp,
            last_updated_timestamp=data_source.last_updated_timestamp,
        )


SUPPORTED_MESSAGE_FORMATS = [
    AvroFormatModel,
    JsonFormatModel,
    ProtoFormatModel,
    ConfluentAvroFormatModel,
]
SUPPORTED_KAFKA_BATCH_SOURCES = [SparkSourceModel]


class KafkaSourceModel(DataSourceModel):
    """
    Pydantic Model of a Feast KafkaSource.
    """

    model_config = ConfigDict(protected_namespaces=())

    name: str
    model_type: Literal["KafkaSourceModel"] = "KafkaSourceModel"
    timestamp_field: str
    message_format: AnyStreamFormat
    kafka_bootstrap_servers: Optional[str] = None
    topic: Optional[str] = None
    created_timestamp_column: Optional[str] = ""
    field_mapping: Optional[Dict[str, str]] = None
    description: Optional[str] = ""
    tags: Optional[Dict[str, str]] = None
    owner: Optional[str] = ""
    batch_source: Optional[AnyBatchDataSource] = None
    watermark_delay_threshold: Optional[timedelta] = None

    # To make it compatible with Pydantic V1, we need this field_serializer
    @field_serializer("watermark_delay_threshold")
    def serialize_ttl(self, ttl: timedelta):
        return timedelta.total_seconds(ttl) if ttl else None

    # To make it compatible with Pydantic V1, we need this field_validator
    @field_validator("watermark_delay_threshold", mode="before")
    @classmethod
    def validate_ttl(cls, v: Optional[Union[int, float, str, timedelta]]):
        try:
            if isinstance(v, timedelta):
                return v
            elif isinstance(v, float):
                return timedelta(seconds=v)
            elif isinstance(v, str):
                return timedelta(seconds=float(v))
            elif isinstance(v, int):
                return timedelta(seconds=v)
            else:
                return timedelta(seconds=0)
        except ValueError:
            raise ValueError("ttl must be one of the int, float, str, timedelta types")

    def to_data_source(self) -> KafkaSource:
        """
        Given a Pydantic KafkaSourceModel, create and return a KafkaSource.

        Returns:
            A KafkaSource.
        """
        source = KafkaSource(
            name=self.name,
            timestamp_field=self.timestamp_field,
            message_format=self.message_format.to_stream_format(),
            kafka_bootstrap_servers=self.kafka_bootstrap_servers,
            topic=self.topic,
            created_timestamp_column=self.created_timestamp_column,
            field_mapping=self.field_mapping,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            batch_source=(
                self.batch_source.to_data_source() if self.batch_source else None
            ),
            watermark_delay_threshold=self.watermark_delay_threshold,
        )
        return self._attach_timestamps(source)

    @classmethod
    def from_data_source(
        cls,
        data_source,
    ) -> Self:  # type: ignore
        """
        Converts a KafkaSource object to its pydantic model representation.

        Returns:
            A KafkaSourceModel.
        """

        class_ = getattr(
            sys.modules[__name__],
            type(data_source.kafka_options.message_format).__name__ + "Model",
        )
        if class_ not in SUPPORTED_MESSAGE_FORMATS:
            raise ValueError(
                "Data Source message format is not a supported stream format."
            )
        message_format = class_.from_stream_format(
            data_source.kafka_options.message_format
        )

        batch_source = None
        if data_source.batch_source:
            class_ = getattr(
                sys.modules[__name__],
                type(data_source.batch_source).__name__ + "Model",
            )
            if class_ not in SUPPORTED_KAFKA_BATCH_SOURCES:
                raise ValueError(
                    "Kafka Source's batch source type is not a supported data source type."
                )
            batch_source = class_.from_data_source(data_source.batch_source)

        return cls(
            name=data_source.name,
            timestamp_field=data_source.timestamp_field,
            message_format=message_format,
            kafka_bootstrap_servers=(
                data_source.kafka_options.kafka_bootstrap_servers
                if data_source.kafka_options.kafka_bootstrap_servers
                else ""
            ),
            topic=(
                data_source.kafka_options.topic
                if data_source.kafka_options.topic
                else ""
            ),
            created_timestamp_column=data_source.created_timestamp_column,
            field_mapping=(
                data_source.field_mapping if data_source.field_mapping else None
            ),
            description=data_source.description,
            tags=data_source.tags if data_source.tags else None,
            owner=data_source.owner,
            batch_source=batch_source,
            watermark_delay_threshold=data_source.kafka_options.watermark_delay_threshold,
            created_timestamp=data_source.created_timestamp,
            last_updated_timestamp=data_source.last_updated_timestamp,
        )


# https://blog.devgenius.io/deserialize-child-classes-with-pydantic-that-gonna-work-784230e1cf83
# This lets us discriminate child classes of DataSourceModel with type hints.
AnyDataSource = Annotated[
    Union[RequestSourceModel, SparkSourceModel, KafkaSourceModel, PushSourceModel],
    PydanticField(discriminator="model_type"),
]
