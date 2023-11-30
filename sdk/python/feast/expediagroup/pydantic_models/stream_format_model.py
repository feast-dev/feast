from typing import Dict, Literal, Optional, Union

from pydantic import BaseModel
from pydantic import Field as PydanticField
from typing_extensions import Annotated, Self

from feast.data_format import (
    StreamFormat,
    AvroFormat,
    JsonFormat,
    ProtoFormat
)


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
    schoma: str

    def to_stream_format(self) -> AvroFormat:
        """
        Given a Pydantic AvroFormatModel, create and return an AvroFormat.

        Returns:
            An AvroFormat.
        """
        return AvroFormat(
            schema_json=self.schoma
        )

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
        return cls(
            schoma=avro_format.schema_json
        )


class JsonFormatModel(StreamFormatModel):
    """
    Pydantic Model of a Feast JsonFormat.
    """

    format: Literal["JsonFormatModel"] = "JsonFormatModel"
    schoma: str

    def to_stream_format(self) -> JsonFormat:
        """
        Given a Pydantic JsonFormatModel, create and return a JsonFormat.

        Returns:
            A JsonFormat.
        """
        return JsonFormat(
            schema_json=self.schoma
        )

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
        return cls(
            schoma=json_format.schema_json
        )


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
        return ProtoFormat(
            class_path=self.class_path
        )

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
        return cls(
            class_path=proto_format.class_path
        )


# https://blog.devgenius.io/deserialize-child-classes-with-pydantic-that-gonna-work-784230e1cf83
# This lets us discriminate child classes of DataSourceModel with type hints.
AnyStreamFormat = Annotated[
    Union[AvroFormatModel, JsonFormatModel, ProtoFormatModel],
    PydanticField(discriminator="format"),
]