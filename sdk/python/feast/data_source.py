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
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Iterable, Optional, Tuple

from feast import type_map
from feast.data_format import StreamFormat
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig, get_data_source_class_from_type
from feast.value_type import ValueType


class SourceType(enum.Enum):
    """
    DataSource value type. Used to define source types in DataSource.
    """

    UNKNOWN = 0
    BATCH_FILE = 1
    BATCH_BIGQUERY = 2
    STREAM_KAFKA = 3
    STREAM_KINESIS = 4


class KafkaOptions:
    """
    DataSource Kafka options used to source features from Kafka messages
    """

    def __init__(
        self, bootstrap_servers: str, message_format: StreamFormat, topic: str,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._message_format = message_format
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
    def message_format(self):
        """
        Returns the data format that is used to encode the feature data in Kafka messages
        """
        return self._message_format

    @message_format.setter
    def message_format(self, message_format):
        """
        Sets the data format that is used to encode the feature data in Kafka messages
        """
        self._message_format = message_format

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
    def from_proto(cls, kafka_options_proto: DataSourceProto.KafkaOptions):
        """
        Creates a KafkaOptions from a protobuf representation of a kafka option

        Args:
            kafka_options_proto: A protobuf representation of a DataSource

        Returns:
            Returns a BigQueryOptions object based on the kafka_options protobuf
        """

        kafka_options = cls(
            bootstrap_servers=kafka_options_proto.bootstrap_servers,
            message_format=StreamFormat.from_proto(kafka_options_proto.message_format),
            topic=kafka_options_proto.topic,
        )

        return kafka_options

    def to_proto(self) -> DataSourceProto.KafkaOptions:
        """
        Converts an KafkaOptionsProto object to its protobuf representation.

        Returns:
            KafkaOptionsProto protobuf
        """

        kafka_options_proto = DataSourceProto.KafkaOptions(
            bootstrap_servers=self.bootstrap_servers,
            message_format=self.message_format.to_proto(),
            topic=self.topic,
        )

        return kafka_options_proto


class KinesisOptions:
    """
    DataSource Kinesis options used to source features from Kinesis records
    """

    def __init__(
        self, record_format: StreamFormat, region: str, stream_name: str,
    ):
        self._record_format = record_format
        self._region = region
        self._stream_name = stream_name

    @property
    def record_format(self):
        """
        Returns the data format used to encode the feature data in the Kinesis records.
        """
        return self._record_format

    @record_format.setter
    def record_format(self, record_format):
        """
        Sets the data format used to encode the feature data in the Kinesis records.
        """
        self._record_format = record_format

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
    def from_proto(cls, kinesis_options_proto: DataSourceProto.KinesisOptions):
        """
        Creates a KinesisOptions from a protobuf representation of a kinesis option

        Args:
            kinesis_options_proto: A protobuf representation of a DataSource

        Returns:
            Returns a KinesisOptions object based on the kinesis_options protobuf
        """

        kinesis_options = cls(
            record_format=StreamFormat.from_proto(kinesis_options_proto.record_format),
            region=kinesis_options_proto.region,
            stream_name=kinesis_options_proto.stream_name,
        )

        return kinesis_options

    def to_proto(self) -> DataSourceProto.KinesisOptions:
        """
        Converts an KinesisOptionsProto object to its protobuf representation.

        Returns:
            KinesisOptionsProto protobuf
        """

        kinesis_options_proto = DataSourceProto.KinesisOptions(
            record_format=self.record_format.to_proto(),
            region=self.region,
            stream_name=self.stream_name,
        )

        return kinesis_options_proto


class DataSource(ABC):
    """
    DataSource that can be used to source features.

    Args:
        event_timestamp_column (optional): Event timestamp column used for point in time
            joins of feature values.
        created_timestamp_column (optional): Timestamp column indicating when the row
            was created, used for deduplicating rows.
        field_mapping (optional): A dictionary mapping of column names in this data
            source to feature names in a feature table or view. Only used for feature
            columns, not entity or timestamp columns.
        date_partition_column (optional): Timestamp column used for partitioning.
    """

    _event_timestamp_column: str
    _created_timestamp_column: str
    _field_mapping: Dict[str, str]
    _date_partition_column: str

    def __init__(
        self,
        event_timestamp_column: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = None,
    ):
        """Creates a DataSource object."""
        self._event_timestamp_column = (
            event_timestamp_column if event_timestamp_column else ""
        )
        self._created_timestamp_column = (
            created_timestamp_column if created_timestamp_column else ""
        )
        self._field_mapping = field_mapping if field_mapping else {}
        self._date_partition_column = (
            date_partition_column if date_partition_column else ""
        )

    def __eq__(self, other):
        if not isinstance(other, DataSource):
            raise TypeError("Comparisons should only involve DataSource class objects.")

        if (
            self.event_timestamp_column != other.event_timestamp_column
            or self.created_timestamp_column != other.created_timestamp_column
            or self.field_mapping != other.field_mapping
            or self.date_partition_column != other.date_partition_column
        ):
            return False

        return True

    @property
    def field_mapping(self) -> Dict[str, str]:
        """
        Returns the field mapping of this data source.
        """
        return self._field_mapping

    @field_mapping.setter
    def field_mapping(self, field_mapping):
        """
        Sets the field mapping of this data source.
        """
        self._field_mapping = field_mapping

    @property
    def event_timestamp_column(self) -> str:
        """
        Returns the event timestamp column of this data source.
        """
        return self._event_timestamp_column

    @event_timestamp_column.setter
    def event_timestamp_column(self, event_timestamp_column):
        """
        Sets the event timestamp column of this data source.
        """
        self._event_timestamp_column = event_timestamp_column

    @property
    def created_timestamp_column(self) -> str:
        """
        Returns the created timestamp column of this data source.
        """
        return self._created_timestamp_column

    @created_timestamp_column.setter
    def created_timestamp_column(self, created_timestamp_column):
        """
        Sets the created timestamp column of this data source.
        """
        self._created_timestamp_column = created_timestamp_column

    @property
    def date_partition_column(self) -> str:
        """
        Returns the date partition column of this data source.
        """
        return self._date_partition_column

    @date_partition_column.setter
    def date_partition_column(self, date_partition_column):
        """
        Sets the date partition column of this data source.
        """
        self._date_partition_column = date_partition_column

    @staticmethod
    @abstractmethod
    def from_proto(data_source: DataSourceProto) -> Any:
        """
        Converts data source config in protobuf spec to a DataSource class object.

        Args:
            data_source: A protobuf representation of a DataSource.

        Returns:
            A DataSource class object.

        Raises:
            ValueError: The type of DataSource could not be identified.
        """
        if data_source.data_source_class_type:
            cls = get_data_source_class_from_type(data_source.data_source_class_type)
            return cls.from_proto(data_source)

        if data_source.file_options.file_format and data_source.file_options.file_url:
            from feast.infra.offline_stores.file_source import FileSource

            data_source_obj = FileSource.from_proto(data_source)
        elif (
            data_source.bigquery_options.table_ref or data_source.bigquery_options.query
        ):
            from feast.infra.offline_stores.bigquery_source import BigQuerySource

            data_source_obj = BigQuerySource.from_proto(data_source)
        elif data_source.redshift_options.table or data_source.redshift_options.query:
            from feast.infra.offline_stores.redshift_source import RedshiftSource

            data_source_obj = RedshiftSource.from_proto(data_source)

        elif data_source.snowflake_options.table or data_source.snowflake_options.query:
            from feast.infra.offline_stores.snowflake_source import SnowflakeSource

            data_source_obj = SnowflakeSource.from_proto(data_source)

        elif (
            data_source.kafka_options.bootstrap_servers
            and data_source.kafka_options.topic
            and data_source.kafka_options.message_format
        ):
            data_source_obj = KafkaSource.from_proto(data_source)
        elif (
            data_source.kinesis_options.record_format
            and data_source.kinesis_options.region
            and data_source.kinesis_options.stream_name
        ):
            data_source_obj = KinesisSource.from_proto(data_source)
        else:
            raise ValueError("Could not identify the source type being added.")

        return data_source_obj

    @abstractmethod
    def to_proto(self) -> DataSourceProto:
        """
        Converts an DataSourceProto object to its protobuf representation.
        """
        raise NotImplementedError

    def validate(self, config: RepoConfig):
        """
        Validates the underlying data source.

        Args:
            config: Configuration object used to configure a feature store.
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        """
        Returns the callable method that returns Feast type given the raw column type.
        """
        raise NotImplementedError

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """
        Returns the list of column names and raw column types.

        Args:
            config: Configuration object used to configure a feature store.
        """
        raise NotImplementedError

    def get_table_query_string(self) -> str:
        """
        Returns a string that can directly be used to reference this table in SQL.
        """
        raise NotImplementedError


class KafkaSource(DataSource):
    def validate(self, config: RepoConfig):
        pass

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        pass

    def __init__(
        self,
        event_timestamp_column: str,
        bootstrap_servers: str,
        message_format: StreamFormat,
        topic: str,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )
        self._kafka_options = KafkaOptions(
            bootstrap_servers=bootstrap_servers,
            message_format=message_format,
            topic=topic,
        )

    def __eq__(self, other):
        if not isinstance(other, KafkaSource):
            raise TypeError(
                "Comparisons should only involve KafkaSource class objects."
            )

        if (
            self.kafka_options.bootstrap_servers
            != other.kafka_options.bootstrap_servers
            or self.kafka_options.message_format != other.kafka_options.message_format
            or self.kafka_options.topic != other.kafka_options.topic
        ):
            return False

        return True

    @property
    def kafka_options(self):
        """
        Returns the kafka options of this data source
        """
        return self._kafka_options

    @kafka_options.setter
    def kafka_options(self, kafka_options):
        """
        Sets the kafka options of this data source
        """
        self._kafka_options = kafka_options

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        return KafkaSource(
            field_mapping=dict(data_source.field_mapping),
            bootstrap_servers=data_source.kafka_options.bootstrap_servers,
            message_format=StreamFormat.from_proto(
                data_source.kafka_options.message_format
            ),
            topic=data_source.kafka_options.topic,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
        )

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.STREAM_KAFKA,
            field_mapping=self.field_mapping,
            kafka_options=self.kafka_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.redshift_to_feast_value_type


class RequestDataSource(DataSource):
    """
    RequestDataSource that can be used to provide input features for on demand transforms

    Args:
        name: Name of the request data source
        schema: Schema mapping from the input feature name to a ValueType
    """

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        raise NotImplementedError

    _name: str
    _schema: Dict[str, ValueType]

    def __init__(
        self, name: str, schema: Dict[str, ValueType],
    ):
        """Creates a RequestDataSource object."""
        super().__init__()
        self._name = name
        self._schema = schema

    @property
    def name(self) -> str:
        """
        Returns the name of this data source
        """
        return self._name

    @property
    def schema(self) -> Dict[str, ValueType]:
        """
        Returns the schema for this request data source
        """
        return self._schema

    def validate(self, config: RepoConfig):
        pass

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        pass

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        schema_pb = data_source.request_data_options.schema
        schema = {}
        for key in schema_pb.keys():
            schema[key] = ValueType(schema_pb.get(key))
        return RequestDataSource(
            name=data_source.request_data_options.name, schema=schema
        )

    def to_proto(self) -> DataSourceProto:
        schema_pb = {}
        for key, value in self._schema.items():
            schema_pb[key] = value.value
        options = DataSourceProto.RequestDataOptions(name=self._name, schema=schema_pb)
        data_source_proto = DataSourceProto(
            type=DataSourceProto.REQUEST_SOURCE, request_data_options=options
        )

        return data_source_proto


class KinesisSource(DataSource):
    def validate(self, config: RepoConfig):
        pass

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        pass

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        return KinesisSource(
            field_mapping=dict(data_source.field_mapping),
            record_format=StreamFormat.from_proto(
                data_source.kinesis_options.record_format
            ),
            region=data_source.kinesis_options.region,
            stream_name=data_source.kinesis_options.stream_name,
            event_timestamp_column=data_source.event_timestamp_column,
            created_timestamp_column=data_source.created_timestamp_column,
            date_partition_column=data_source.date_partition_column,
        )

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        pass

    def __init__(
        self,
        event_timestamp_column: str,
        created_timestamp_column: str,
        record_format: StreamFormat,
        region: str,
        stream_name: str,
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )
        self._kinesis_options = KinesisOptions(
            record_format=record_format, region=region, stream_name=stream_name
        )

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, KinesisSource):
            raise TypeError(
                "Comparisons should only involve KinesisSource class objects."
            )

        if (
            self.kinesis_options.record_format != other.kinesis_options.record_format
            or self.kinesis_options.region != other.kinesis_options.region
            or self.kinesis_options.stream_name != other.kinesis_options.stream_name
        ):
            return False

        return True

    @property
    def kinesis_options(self):
        """
        Returns the kinesis options of this data source
        """
        return self._kinesis_options

    @kinesis_options.setter
    def kinesis_options(self, kinesis_options):
        """
        Sets the kinesis options of this data source
        """
        self._kinesis_options = kinesis_options

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.STREAM_KINESIS,
            field_mapping=self.field_mapping,
            kinesis_options=self.kinesis_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto
