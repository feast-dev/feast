from typing import Literal, Union

from pydantic import BaseModel
from pydantic import Field as PydanticField
from typing_extensions import Annotated, Self

from feast.data_format import AvroFormat, ConfluentAvroFormat, JsonFormat, ProtoFormat


class StreamFormatModel(BaseModel):
    """
    Pydantic Model of a Feast StreamFormat.
    """

    def to_stream_format(self):
        """
        Given a Pydantic StreamFormatModel, create and return a StreamFormat.

        Returns:
            A StreamFormat.
        """
        raise NotImplementedError

    @classmethod
    def from_stream_format(cls, stream_format):
        """
        Converts a StreamFormat object to its pydantic model representation.

        Returns:
            A StreamFormatModel.
        """
        raise NotImplementedError


class AvroFormatModel(StreamFormatModel):
    """
    Pydantic Model of a Feast AvroFormat.
    """

    format: Literal["AvroFormatModel"] = "AvroFormatModel"
    schema_str: str

    def to_stream_format(self) -> AvroFormat:
        """
        Given a Pydantic AvroFormatModel, create and return an AvroFormat.

        Returns:
            An AvroFormat.
        """
        return AvroFormat(schema_json=self.schema_str)

    @classmethod
    def from_stream_format(
        cls,
        avro_format,
    ) -> Self:  # type: ignore
        """
        Converts an AvroFormat object to its pydantic model representation.

        Returns:
            An AvroFormatModel.
        """
        return cls(schema_str=avro_format.schema_json)


class JsonFormatModel(StreamFormatModel):
    """
    Pydantic Model of a Feast JsonFormat.
    """

    format: Literal["JsonFormatModel"] = "JsonFormatModel"
    schema_str: str

    def to_stream_format(self) -> JsonFormat:
        """
        Given a Pydantic JsonFormatModel, create and return a JsonFormat.

        Returns:
            A JsonFormat.
        """
        return JsonFormat(schema_json=self.schema_str)

    @classmethod
    def from_stream_format(
        cls,
        json_format,
    ) -> Self:  # type: ignore
        """
        Converts a JsonFormat object to its pydantic model representation.

        Returns:
            A JsonFormatModel.
        """
        return cls(schema_str=json_format.schema_json)


class ProtoFormatModel(StreamFormatModel):
    """
    Pydantic Model of a Feast ProtoFormat.
    """

    format: Literal["ProtoFormatModel"] = "ProtoFormatModel"
    class_path: str

    def to_stream_format(self) -> ProtoFormat:
        """
        Given a Pydantic ProtoFormatModel, create and return a ProtoFormat.

        Returns:
            A ProtoFormat.
        """
        return ProtoFormat(class_path=self.class_path)

    @classmethod
    def from_stream_format(
        cls,
        proto_format,
    ) -> Self:  # type: ignore
        """
        Converts a ProtoFormat object to its pydantic model representation.

        Returns:
            A ProtoFormatModel.
        """
        return cls(class_path=proto_format.class_path)


class ConfluentAvroFormatModel(StreamFormatModel):
    """
    Pydantic Model of a Feast ProtoFormat.
    """

    format: Literal["ConfluentAvroFormatModel"] = "ConfluentAvroFormatModel"
    record_name: str
    record_namespace: str

    def to_stream_format(self) -> ConfluentAvroFormat:
        """
        Given a Pydantic ProtoFormatModel, create and return a ProtoFormat.

        Returns:
            A ProtoFormat.
        """
        return ConfluentAvroFormat(
            record_name=self.record_name, record_namespace=self.record_namespace
        )

    @classmethod
    def from_stream_format(
        cls,
        confluent_avro_format,
    ) -> Self:  # type: ignore
        """
        Converts a ProtoFormat object to its pydantic model representation.

        Returns:
            A ProtoFormatModel.
        """
        return cls(
            record_name=confluent_avro_format.record_name,
            record_namespace=confluent_avro_format.record_namespace,
        )


# https://blog.devgenius.io/deserialize-child-classes-with-pydantic-that-gonna-work-784230e1cf83
# This lets us discriminate child classes of DataSourceModel with type hints.
AnyStreamFormat = Annotated[
    Union[AvroFormatModel, JsonFormatModel, ProtoFormatModel, ConfluentAvroFormatModel],
    PydanticField(discriminator="format"),
]
