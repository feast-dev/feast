# Copyright 2020 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY aIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from abc import ABC, abstractmethod

from feast.protos.feast.core.DataFormat_pb2 import FileFormat as FileFormatProto
from feast.protos.feast.core.DataFormat_pb2 import StreamFormat as StreamFormatProto
from feast.protos.feast.core.DataFormat_pb2 import TableFormat as TableFormatProto


class FileFormat(ABC):
    """
    Defines an abtract file forma used to encode feature data in files
    """

    @abstractmethod
    def to_proto(self):
        """
        Convert this FileFormat into its protobuf representation.
        """
        pass

    def __eq__(self, other):
        return self.to_proto() == other.to_proto()

    @classmethod
    def from_proto(cls, proto):
        """
        Construct this FileFormat from its protobuf representation.
        Raises NotImplementedError if FileFormat specified in given proto is not supported.
        """
        fmt = proto.WhichOneof("format")
        if fmt == "parquet_format":
            return ParquetFormat()
        elif fmt == "delta_format":
            return DeltaFormat()
        if fmt is None:
            return None
        raise NotImplementedError(f"FileFormat is unsupported: {fmt}")

    def __str__(self):
        """
        String representation of the file format passed to spark
        """
        raise NotImplementedError()


class ParquetFormat(FileFormat):
    """
    Defines the Parquet data format
    """

    def to_proto(self):
        return FileFormatProto(parquet_format=FileFormatProto.ParquetFormat())

    def __str__(self):
        return "parquet"


class DeltaFormat(FileFormat):
    """
    Defines delta data format (deprecated - use TableFormat.DeltaFormat instead)
    """

    def to_proto(self):
        # Reference TableFormat.DeltaFormat since DeltaFormat is now nested there
        return FileFormatProto(delta_format=TableFormatProto.DeltaFormat())

    def __str__(self):
        return "delta"


class StreamFormat(ABC):
    """
    Defines an abtracts streaming data format used to encode feature data in streams
    """

    @abstractmethod
    def to_proto(self):
        """
        Convert this StreamFormat into its protobuf representation.
        """
        pass

    def __eq__(self, other):
        return self.to_proto() == other.to_proto()

    @classmethod
    def from_proto(cls, proto):
        """
        Construct this StreamFormat from its protobuf representation.
        """
        fmt = proto.WhichOneof("format")
        if fmt == "avro_format":
            return AvroFormat(schema_json=proto.avro_format.schema_json)
        if fmt == "json_format":
            return JsonFormat(schema_json=proto.json_format.schema_json)
        if fmt == "proto_format":
            return ProtoFormat(class_path=proto.proto_format.class_path)
        raise NotImplementedError(f"StreamFormat is unsupported: {fmt}")


class AvroFormat(StreamFormat):
    """
    Defines the Avro streaming data format that encodes data in Avro format
    """

    def __init__(self, schema_json: str):
        """
        Construct a new Avro data format.

        Args:
            schema_json: Avro schema definition in JSON
        """
        self.schema_json = schema_json

    def to_proto(self):
        proto = StreamFormatProto.AvroFormat(schema_json=self.schema_json)
        return StreamFormatProto(avro_format=proto)


class JsonFormat(StreamFormat):
    """
    Defines the Json streaming data format that encodes data in Json format
    """

    def __init__(self, schema_json: str):
        """
        Construct a new Json data format.

        For spark, uses pyspark ddl string format. Example shown here:
        https://vincent.doba.fr/posts/20211004_spark_data_description_language_for_defining_spark_schema/

        Args:
            schema_json: Json schema definition
        """
        self.schema_json = schema_json

    def to_proto(self):
        proto = StreamFormatProto.JsonFormat(schema_json=self.schema_json)
        return StreamFormatProto(json_format=proto)


class ProtoFormat(StreamFormat):
    """
    Defines the Protobuf data format
    """

    def __init__(self, class_path: str):
        """
        Construct a new Protobuf data format.

        Args:
            class_path: Class path to the Java Protobuf class that can be used to decode protobuf messages.;
        """
        self.class_path = class_path

    def to_proto(self):
        return StreamFormatProto(
            proto_format=StreamFormatProto.ProtoFormat(class_path=self.class_path)
        )
