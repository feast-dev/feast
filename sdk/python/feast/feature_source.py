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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import enum
from typing import MutableMapping, Optional

from feast.core.FeatureSource_pb2 import FeatureSourceSpec as FeatureSourceSpecProto


class SourceType(enum.Enum):
    """
    FeatureSource value type. Used to define source types in FeatureSource.
    """

    UNKNOWN = 0
    BATCH_FILE = 1
    BATCH_BIGQUERY = 2
    STREAM_KAFKA = 3
    STREAM_KINESIS = 4


class FileFormat(enum.Enum):
    """
    FileOptions value type. Used to define source types in FeatureSource.
    """

    UNKNOWN = 0
    PARQUET = 1
    AVRO = 2


class FeatureSourceSpecOptions:
    """
    FeatureSourceSpec options that can be used to source features
    """

    def to_proto(self):
        """
        Unimplemented to_proto method for a field. This should be extended.
        """
        pass

    def from_proto(self, proto):
        """
        Unimplemented from_proto method for a field. This should be extended.
        """
        pass


class FileOptions(FeatureSourceSpecOptions):
    """
    FeatureSourceSpec File options used to source features from a file
    """

    def __init__(
        self, file_format: FileFormat, file_url: str,
    ):
        self._file_format = file_format
        self._file_url = file_url

    @property
    def file_format(self):
        """
        Returns the file format of this file
        """
        return self._file_format

    @file_format.setter
    def file_format(self, file_format):
        """
        Sets the file format of this file
        """
        self._file_format = file_format

    @property
    def file_url(self):
        """
        Returns the file url of this file
        """
        return self._file_url

    @file_url.setter
    def file_url(self, file_url):
        """
        Sets the file url of this file
        """
        self._file_url = file_url

    @classmethod
    def from_proto(cls, file_options_proto: FeatureSourceSpecProto.FileOptions):
        """
        Creates a FileOptions from a protobuf representation of a file option

        Args:
            file_options_proto: A protobuf representation of a FeatureSourceSpec

        Returns:
            Returns a FileOptions object based on the file_options protobuf
        """

        file_options = cls(
            file_format=FileFormat(file_options_proto.file_format),
            file_url=file_options_proto.file_url,
        )

        return file_options

    def to_proto(self) -> FeatureSourceSpecProto.FileOptions:
        """
        Converts an FileOptionsProto object to its protobuf representation.

        Returns:
            FileOptionsProto protobuf
        """

        file_options_proto = FeatureSourceSpecProto.FileOptions(
            file_format=self.file_format, file_url=self.file_url,
        )

        return file_options_proto


class BigQueryOptions(FeatureSourceSpecOptions):
    """
    FeatureSourceSpec BigQuery options used to source features from BigQuery query
    """

    def __init__(
        self, project_id: str, sql_query: str,
    ):
        self._project_id = project_id
        self._sql_query = sql_query

    @property
    def project_id(self):
        """
        Returns the project id of this file
        """
        return self._project_id

    @project_id.setter
    def project_id(self, project_id):
        """
        Sets the project id of this file
        """
        self._project_id = project_id

    @property
    def sql_query(self):
        """
        Returns the sql query of this file
        """
        return self._sql_query

    @sql_query.setter
    def sql_query(self, sql_query):
        """
        Sets the sql query of this file
        """
        self._sql_query = sql_query

    @classmethod
    def from_proto(cls, bigquery_options_proto: FeatureSourceSpecProto.BigQueryOptions):
        """
        Creates a BigQueryOptions from a protobuf representation of a BigQuery option

        Args:
            bigquery_options_proto: A protobuf representation of a FeatureSourceSpec

        Returns:
            Returns a BigQueryOptions object based on the bigquery_options protobuf
        """

        bigquery_options = cls(
            project_id=bigquery_options_proto.project_id,
            sql_query=bigquery_options_proto.sql_query,
        )

        return bigquery_options

    def to_proto(self) -> FeatureSourceSpecProto.BigQueryOptions:
        """
        Converts an BigQueryOptionsProto object to its protobuf representation.

        Returns:
            BigQueryOptionsProto protobuf
        """

        bigquery_options_proto = FeatureSourceSpecProto.BigQueryOptions(
            project_id=self.project_id, sql_query=self.sql_query,
        )

        return bigquery_options_proto


class KafkaOptions(FeatureSourceSpecOptions):
    """
    FeatureSourceSpec Kafka options used to source features from Kafka messages
    """

    def __init__(
        self, bootstrap_servers: str, class_path: str, topic: str,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._class_path = class_path
        self._topic = topic

    @property
    def bootstrap_servers(self):
        """
        Returns a comma-separated list of Kafka bootstrap servers
        """
        return self._bootstrap_servers

    @bootstrap_servers.setter
    def bootstrap_servers(self, bootstrap_servers):
        """
        Sets a comma-separated list of Kafka bootstrap servers
        """
        self._bootstrap_servers = bootstrap_servers

    @property
    def class_path(self):
        """
        Returns the class path to the generated Java Protobuf class that can be
        used to decode feature data from the obtained Kafka message
        """
        return self._class_path

    @class_path.setter
    def class_path(self, class_path):
        """
        Sets the class path to the generated Java Protobuf class that can be
        used to decode feature data from the obtained Kafka message
        """
        self._class_path = class_path

    @property
    def topic(self):
        """
        Returns the Kafka topic to collect feature data from
        """
        return self._topic

    @topic.setter
    def topic(self, topic):
        """
        Sets the Kafka topic to collect feature data from
        """
        self._topic = topic

    @classmethod
    def from_proto(cls, kafka_options_proto: FeatureSourceSpecProto.KafkaOptions):
        """
        Creates a KafkaOptions from a protobuf representation of a kafka option

        Args:
            kafka_options_proto: A protobuf representation of a FeatureSourceSpec

        Returns:
            Returns a BigQueryOptions object based on the kafka_options protobuf
        """

        kafka_options = cls(
            bootstrap_servers=kafka_options_proto.bootstrap_servers,
            class_path=kafka_options_proto.class_path,
            topic=kafka_options_proto.topic,
        )

        return kafka_options

    def to_proto(self) -> FeatureSourceSpecProto.KafkaOptions:
        """
        Converts an KafkaOptionsProto object to its protobuf representation.

        Returns:
            KafkaOptionsProto protobuf
        """

        kafka_options_proto = FeatureSourceSpecProto.KafkaOptions(
            bootstrap_servers=self.bootstrap_servers,
            class_path=self.class_path,
            topic=self.topic,
        )

        return kafka_options_proto


class KinesisOptions(FeatureSourceSpecOptions):
    """
    FeatureSourceSpec Kinesis options used to source features from Kinesis records
    """

    def __init__(
        self, class_path: str, region: str, stream_name: str,
    ):
        self._class_path = class_path
        self._region = region
        self._stream_name = stream_name

    @property
    def class_path(self):
        """
        Returns the class path to the generated Java Protobuf class that can be
        used to decode feature data from the obtained Kinesis record
        """
        return self._class_path

    @class_path.setter
    def class_path(self, class_path):
        """
        Sets the class path to the generated Java Protobuf class that can be
        used to decode feature data from the obtained Kinesis record
        """
        self._class_path = class_path

    @property
    def region(self):
        """
        Returns the AWS region of Kinesis stream
        """
        return self._region

    @region.setter
    def region(self, region):
        """
        Sets the AWS region of Kinesis stream
        """
        self._region = region

    @property
    def stream_name(self):
        """
        Returns the Kinesis stream name to obtain feature data from
        """
        return self._stream_name

    @stream_name.setter
    def stream_name(self, stream_name):
        """
        Sets the Kinesis stream name to obtain feature data from
        """
        self._stream_name = stream_name

    @classmethod
    def from_proto(cls, kinesis_options_proto: FeatureSourceSpecProto.KinesisOptions):
        """
        Creates a KinesisOptions from a protobuf representation of a kinesis option

        Args:
            kinesis_options_proto: A protobuf representation of a FeatureSourceSpec

        Returns:
            Returns a KinesisOptions object based on the kinesis_options protobuf
        """

        kinesis_options = cls(
            class_path=kinesis_options_proto.class_path,
            region=kinesis_options_proto.region,
            stream_name=kinesis_options_proto.stream_name,
        )

        return kinesis_options

    def to_proto(self) -> FeatureSourceSpecProto.KinesisOptions:
        """
        Converts an KinesisOptionsProto object to its protobuf representation.

        Returns:
            KinesisOptionsProto protobuf
        """

        kinesis_options_proto = FeatureSourceSpecProto.KinesisOptions(
            class_path=self.class_path,
            region=self.region,
            stream_name=self.stream_name,
        )

        return kinesis_options_proto


class FeatureSourceSpec:
    """
    FeatureSourceSpec that can be used source features
    """

    def __init__(
        self,
        type: str,
        field_mapping: MutableMapping[str, str],
        options: FeatureSourceSpecOptions,
        ts_column: str,
        date_partition_column: Optional[str] = "",
    ):
        self._type = type
        self._field_mapping = field_mapping
        self._options = options
        self._ts_column = ts_column
        self._date_partition_column = date_partition_column

    @property
    def type(self):
        """
        Returns the type of this feature source
        """
        return self._type

    @type.setter
    def type(self, type):
        """
        Sets the type of this feature source
        """
        self._type = type

    @property
    def field_mapping(self):
        """
        Returns the field mapping of this feature source
        """
        return self._field_mapping

    @field_mapping.setter
    def field_mapping(self, field_mapping):
        """
        Sets the field mapping of this feature source
        """
        self._field_mapping = field_mapping

    @property
    def options(self):
        """
        Returns the options of this feature source
        """
        return self._options

    @options.setter
    def options(self, options):
        """
        Sets the options of this feature source
        """
        self._options = options

    @property
    def ts_column(self):
        """
        Returns the timestamp column of this feature source
        """
        return self._ts_column

    @ts_column.setter
    def ts_column(self, ts_column):
        """
        Sets the timestamp column of this feature source
        """
        self._ts_column = ts_column

    @property
    def date_partition_column(self):
        """
        Returns the date partition column of this feature source
        """
        return self._date_partition_column

    @date_partition_column.setter
    def date_partition_column(self, date_partition_column):
        """
        Sets the date partition column of this feature source
        """
        self._date_partition_column = date_partition_column

    @classmethod
    def from_proto(cls, feature_source_proto: FeatureSourceSpecProto):
        """
        Creates a FeatureSourceSpec from a protobuf representation of an feature source

        Args:
            feature_source_proto: A protobuf representation of a FeatureSourceSpec

        Returns:
            Returns a FeatureSourceSpec object based on the feature_source protobuf
        """

        if isinstance(cls.options, FileOptions):
            feature_source = cls(file_options=feature_source_proto.options,)
        if isinstance(cls.options, BigQueryOptions):
            feature_source = cls(bigquery_options=feature_source_proto.options,)
        if isinstance(cls.options, KafkaOptions):
            feature_source = cls(kafka_options=feature_source_proto.options,)
        if isinstance(cls.options, KinesisOptions):
            feature_source = cls(kinesis_options=feature_source_proto.options,)
        else:
            raise TypeError(
                "FeatureSourceSpec.from_proto: Provided Feature Source option is invalid. Only FileOptions, BigQueryOptions, KafkaOptions and KinesisOptions are supported currently."
            )

        feature_source = cls(
            type=feature_source_proto.type,
            field_mapping=feature_source_proto.field_mapping,
            ts_column=feature_source_proto.ts_column,
            date_partition_column=feature_source_proto.date_partition_column,
        )

        return feature_source

    def to_proto(self) -> FeatureSourceSpecProto:
        """
        Converts an FeatureSourceSpecProto object to its protobuf representation.
        Used when passing FeatureSourceSpecProto object to Feast request.

        Returns:
            FeatureSourceSpecProto protobuf
        """

        if isinstance(self.options, FileOptions):
            feature_source_proto = FeatureSourceSpecProto(
                type=self.type,
                field_mapping=self.field_mapping,
                file_options=self.options.to_proto(),
            )
        elif isinstance(self.options, BigQueryOptions):
            feature_source_proto = FeatureSourceSpecProto(
                type=self.type,
                field_mapping=self.field_mapping,
                bigquery_options=self.options.to_proto(),
            )
        elif isinstance(self.options, KafkaOptions):
            feature_source_proto = FeatureSourceSpecProto(
                type=self.type,
                field_mapping=self.field_mapping,
                kafka_options=self.options.to_proto(),
            )
        elif isinstance(self.options, KinesisOptions):
            feature_source_proto = FeatureSourceSpecProto(
                type=self.type,
                field_mapping=self.field_mapping,
                kinesis_options=self.options.to_proto(),
            )
        else:
            raise TypeError(
                "FeatureSourceSpec.to_proto: Provided Feature Source option is invalid. Only FileOptions, BigQueryOptions, KafkaOptions and KinesisOptions are supported currently."
            )

        feature_source_proto.ts_column = self.ts_column
        feature_source_proto.date_partition_column = self.date_partition_column

        return feature_source_proto
