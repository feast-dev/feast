# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel, Field, model_validator


class FileFormat(BaseModel):
    """
    Defines the file format encoding the features/entity data in files
    """

    class ParquetFormat(BaseModel):
        """
        Defines options for the Parquet data format
        """

    class DeltaFormat(BaseModel):
        """
        Defines options for delta data format
        """

    _one_of_dict = {"FileFormat.format": {"fields": {"delta_format", "parquet_format"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
    parquet_format: "FileFormat.ParquetFormat" = Field(
        default_factory=lambda: FileFormat.ParquetFormat()
    )
    delta_format: "FileFormat.DeltaFormat" = Field(
        default_factory=lambda: FileFormat.DeltaFormat()
    )


class StreamFormat(BaseModel):
    """
    Defines the data format encoding features/entity data in data streams
    """

    class ProtoFormat(BaseModel):
        """
        Defines options for the protobuf data format
        """

        # Classpath to the generated Java Protobuf class that can be used to decode
        # Feature data from the obtained stream message
        class_path: str = Field(default="")

    class AvroFormat(BaseModel):
        """
        Defines options for the avro data format
        """

        # Optional if used in a File DataSource as schema is embedded in avro file.
        # Specifies the schema of the Avro message as JSON string.
        schema_json: str = Field(default="")

    class JsonFormat(BaseModel):
        schema_json: str = Field(default="")

    _one_of_dict = {
        "StreamFormat.format": {
            "fields": {"avro_format", "json_format", "proto_format"}
        }
    }
    one_of_validator = model_validator(mode="before")(check_one_of)
    avro_format: "StreamFormat.AvroFormat" = Field(
        default_factory=lambda: StreamFormat.AvroFormat()
    )
    proto_format: "StreamFormat.ProtoFormat" = Field(
        default_factory=lambda: StreamFormat.ProtoFormat()
    )
    json_format: "StreamFormat.JsonFormat" = Field(
        default_factory=lambda: StreamFormat.JsonFormat()
    )
