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
import warnings
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from google.protobuf.duration_pb2 import Duration
from google.protobuf.json_format import MessageToJson
from typeguard import typechecked

from feast import type_map
from feast.data_format import StreamFormat
from feast.field import Field
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig, get_data_source_class_from_type
from feast.types import from_value_type
from feast.utils import _utc_now
from feast.value_type import ValueType


class KafkaOptions:
    """
    DataSource Kafka options used to source features from Kafka messages
    """

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        message_format: StreamFormat,
        topic: str,
        watermark_delay_threshold: Optional[timedelta] = None,
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.message_format = message_format
        self.topic = topic
        self.watermark_delay_threshold = watermark_delay_threshold or None

    @classmethod
    def from_proto(cls, kafka_options_proto: DataSourceProto.KafkaOptions):
        """
        Creates a KafkaOptions from a protobuf representation of a kafka option

        Args:
            kafka_options_proto: A protobuf representation of a DataSource

        Returns:
            Returns a KafkaOptions object based on the kafka_options protobuf
        """
        watermark_delay_threshold = None
        if kafka_options_proto.HasField("watermark_delay_threshold"):
            watermark_delay_threshold = (
                timedelta(days=0)
                if kafka_options_proto.watermark_delay_threshold.ToNanoseconds() == 0
                else kafka_options_proto.watermark_delay_threshold.ToTimedelta()
            )
        kafka_options = cls(
            kafka_bootstrap_servers=kafka_options_proto.kafka_bootstrap_servers,
            message_format=StreamFormat.from_proto(kafka_options_proto.message_format),
            topic=kafka_options_proto.topic,
            watermark_delay_threshold=watermark_delay_threshold,
        )

        return kafka_options

    def to_proto(self) -> DataSourceProto.KafkaOptions:
        """
        Converts an KafkaOptionsProto object to its protobuf representation.

        Returns:
            KafkaOptionsProto protobuf
        """
        watermark_delay_threshold = None
        if self.watermark_delay_threshold is not None:
            watermark_delay_threshold = Duration()
            watermark_delay_threshold.FromTimedelta(self.watermark_delay_threshold)

        kafka_options_proto = DataSourceProto.KafkaOptions(
            kafka_bootstrap_servers=self.kafka_bootstrap_servers,
            message_format=self.message_format.to_proto(),
            topic=self.topic,
            watermark_delay_threshold=watermark_delay_threshold,
        )

        return kafka_options_proto


class KinesisOptions:
    """
    DataSource Kinesis options used to source features from Kinesis records
    """

    def __init__(
        self,
        record_format: StreamFormat,
        region: str,
        stream_name: str,
    ):
        self.record_format = record_format
        self.region = region
        self.stream_name = stream_name

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


_DATA_SOURCE_OPTIONS = {
    DataSourceProto.SourceType.BATCH_FILE: "feast.infra.offline_stores.file_source.FileSource",
    DataSourceProto.SourceType.BATCH_BIGQUERY: "feast.infra.offline_stores.bigquery_source.BigQuerySource",
    DataSourceProto.SourceType.BATCH_REDSHIFT: "feast.infra.offline_stores.redshift_source.RedshiftSource",
    DataSourceProto.SourceType.BATCH_SNOWFLAKE: "feast.infra.offline_stores.snowflake_source.SnowflakeSource",
    DataSourceProto.SourceType.BATCH_TRINO: "feast.infra.offline_stores.contrib.trino_offline_store.trino_source.TrinoSource",
    DataSourceProto.SourceType.BATCH_SPARK: "feast.infra.offline_stores.contrib.spark_offline_store.spark_source.SparkSource",
    DataSourceProto.SourceType.BATCH_ATHENA: "feast.infra.offline_stores.contrib.athena_offline_store.athena_source.AthenaSource",
    DataSourceProto.SourceType.STREAM_KAFKA: "feast.data_source.KafkaSource",
    DataSourceProto.SourceType.STREAM_KINESIS: "feast.data_source.KinesisSource",
    DataSourceProto.SourceType.REQUEST_SOURCE: "feast.data_source.RequestSource",
    DataSourceProto.SourceType.PUSH_SOURCE: "feast.data_source.PushSource",
}

_DATA_SOURCE_FOR_OFFLINE_STORE = {
    DataSourceProto.SourceType.BATCH_FILE: "feast.infra.offline_stores.dask.DaskOfflineStore",
    DataSourceProto.SourceType.BATCH_BIGQUERY: "feast.infra.offline_stores.bigquery.BigQueryOfflineStore",
    DataSourceProto.SourceType.BATCH_REDSHIFT: "feast.infra.offline_stores.redshift.RedshiftOfflineStore",
    DataSourceProto.SourceType.BATCH_SNOWFLAKE: "feast.infra.offline_stores.snowflake.SnowflakeOfflineStore",
    DataSourceProto.SourceType.BATCH_TRINO: "feast.infra.offline_stores.contrib.trino_offline_store.trino.TrinoOfflineStore",
    DataSourceProto.SourceType.BATCH_SPARK: "feast.infra.offline_stores.contrib.spark_offline_store.spark.SparkOfflineStore",
    DataSourceProto.SourceType.BATCH_ATHENA: "feast.infra.offline_stores.contrib.athena_offline_store.athena.AthenaOfflineStore",
}


@typechecked
class DataSource(ABC):
    """
    DataSource that can be used to source features.

    Args:
        name: Name of data source, which should be unique within a project
        timestamp_field (optional): Event timestamp field used for point-in-time joins of
            feature values.
        created_timestamp_column (optional): Timestamp column indicating when the row
            was created, used for deduplicating rows.
        field_mapping (optional): A dictionary mapping of column names in this data
            source to feature names in a feature table or view. Only used for feature
            columns and timestamp columns, not entity columns.
        description (optional) A human-readable description.
        tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
        owner (optional): The owner of the data source, typically the email of the primary
            maintainer.
        date_partition_column (optional): Timestamp column used for partitioning. Not supported by all offline stores.
        created_timestamp: The time when the data source was created.
        last_updated_timestamp: The time when the data source was last updated.
    """

    name: str
    timestamp_field: str
    created_timestamp_column: str
    field_mapping: Dict[str, str]
    description: str
    tags: Dict[str, str]
    owner: str
    date_partition_column: str
    created_timestamp: Optional[datetime]
    last_updated_timestamp: Optional[datetime]

    def __init__(
        self,
        *,
        name: str,
        timestamp_field: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        date_partition_column: Optional[str] = None,
    ):
        """
        Creates a DataSource object.

        Args:
            name: Name of data source, which should be unique within a project.
            timestamp_field (optional): Event timestamp field used for point-in-time joins of
                feature values.
            created_timestamp_column (optional): Timestamp column indicating when the row
                was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to feature names in a feature table or view. Only used for feature
                columns, not entity or timestamp columns.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the data source, typically the email of the primary
                maintainer.
            date_partition_column (optional): Timestamp column used for partitioning. Not supported by all stores
        """
        self.name = name
        self.timestamp_field = timestamp_field or ""
        self.created_timestamp_column = (
            created_timestamp_column if created_timestamp_column else ""
        )
        self.field_mapping = field_mapping if field_mapping else {}
        if (
            self.timestamp_field
            and self.timestamp_field == self.created_timestamp_column
        ):
            raise ValueError(
                "Please do not use the same column for 'timestamp_field' and 'created_timestamp_column'."
            )
        self.description = description or ""
        self.tags = tags or {}
        self.owner = owner or ""
        self.date_partition_column = (
            date_partition_column if date_partition_column else ""
        )
        now = _utc_now()
        self.created_timestamp = now
        self.last_updated_timestamp = now

    def __hash__(self):
        return hash((self.name, self.timestamp_field))

    def __str__(self):
        return str(MessageToJson(self.to_proto()))

    def __eq__(self, other):
        if other is None:
            return False

        if not isinstance(other, DataSource):
            raise TypeError("Comparisons should only involve DataSource class objects.")

        if (
            self.name != other.name
            or self.timestamp_field != other.timestamp_field
            or self.created_timestamp_column != other.created_timestamp_column
            or self.field_mapping != other.field_mapping
            or self.date_partition_column != other.date_partition_column
            or self.description != other.description
            or self.tags != other.tags
            or self.owner != other.owner
        ):
            return False

        return True

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
        data_source_type = data_source.type
        if not data_source_type or (
            data_source_type
            not in list(_DATA_SOURCE_OPTIONS.keys())
            + [DataSourceProto.SourceType.CUSTOM_SOURCE]
        ):
            raise ValueError("Could not identify the source type being added.")

        if data_source_type == DataSourceProto.SourceType.CUSTOM_SOURCE:
            cls = get_data_source_class_from_type(data_source.data_source_class_type)
            data_source_instance = cls.from_proto(data_source)
        else:
            cls = get_data_source_class_from_type(
                _DATA_SOURCE_OPTIONS[data_source_type]
            )
            data_source_instance = cls.from_proto(data_source)

        data_source_instance._extract_timestamps_from_proto(data_source)

        return data_source_instance

    def to_proto(self) -> DataSourceProto:
        """
        Converts a DataSourceProto object to its protobuf representation.
        """
        proto = self._to_proto_impl()
        self._set_timestamps_in_proto(proto)

        return proto

    @abstractmethod
    def _to_proto_impl(self) -> DataSourceProto:
        """
        Subclass implementation of protobuf conversion.
        This should be implemented by each DataSource subclass.
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

    def _extract_timestamps_from_proto(self, data_source_proto: DataSourceProto):
        """
        Internal method to extract created_timestamp and last_updated_timestamp from protobuf.
        Called automatically by the base from_proto method.
        """
        if data_source_proto.HasField("meta"):
            if data_source_proto.meta.HasField("created_timestamp"):
                self.created_timestamp = (
                    data_source_proto.meta.created_timestamp.ToDatetime().replace(
                        tzinfo=timezone.utc
                    )
                )
            if data_source_proto.meta.HasField("last_updated_timestamp"):
                self.last_updated_timestamp = (
                    data_source_proto.meta.last_updated_timestamp.ToDatetime().replace(
                        tzinfo=timezone.utc
                    )
                )

    def _set_timestamps_in_proto(self, data_source_proto: DataSourceProto):
        """
        Internal method to set created_timestamp and last_updated_timestamp in protobuf.
        Called automatically by the base to_proto method.
        """
        if not data_source_proto.HasField("meta"):
            data_source_proto.meta.CopyFrom(DataSourceProto.SourceMeta())

        if self.created_timestamp:
            data_source_proto.meta.created_timestamp.FromDatetime(
                self.created_timestamp
            )
        if self.last_updated_timestamp:
            data_source_proto.meta.last_updated_timestamp.FromDatetime(
                self.last_updated_timestamp
            )

    @abstractmethod
    def source_type(self) -> DataSourceProto.SourceType.ValueType: ...


@typechecked
class KafkaSource(DataSource):
    """A KafkaSource allow users to register Kafka streams as data sources."""

    def __init__(
        self,
        *,
        name: str,
        timestamp_field: str,
        message_format: StreamFormat,
        bootstrap_servers: Optional[str] = None,
        kafka_bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        batch_source: Optional[DataSource] = None,
        watermark_delay_threshold: Optional[timedelta] = None,
    ):
        """
        Creates a KafkaSource object.

        Args:
            name: Name of data source, which should be unique within a project
            timestamp_field: Event timestamp field used for point-in-time joins of feature values.
            message_format: StreamFormat of serialized messages.
            bootstrap_servers: (Deprecated) The servers of the kafka broker in the form "localhost:9092".
            kafka_bootstrap_servers (optional): The servers of the kafka broker in the form "localhost:9092".
            topic (optional): The name of the topic to read from in the kafka source.
            created_timestamp_column (optional): Timestamp column indicating when the row
                was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to feature names in a feature table or view. Only used for feature
                columns, not entity or timestamp columns.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the data source, typically the email of the primary
                maintainer.
            batch_source (optional): The datasource that acts as a batch source.
            watermark_delay_threshold (optional): The watermark delay threshold for stream data.
                Specifically how late stream data can arrive without being discarded.
        """
        if bootstrap_servers:
            warnings.warn(
                (
                    "The 'bootstrap_servers' parameter has been deprecated in favor of 'kafka_bootstrap_servers'. "
                    "Feast 0.25 and onwards will not support the 'bootstrap_servers' parameter."
                ),
                DeprecationWarning,
            )

        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )
        self.batch_source = batch_source

        kafka_bootstrap_servers = kafka_bootstrap_servers or bootstrap_servers or ""
        topic = topic or ""

        self.kafka_options = KafkaOptions(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            message_format=message_format,
            topic=topic,
            watermark_delay_threshold=watermark_delay_threshold,
        )

    def __eq__(self, other):
        if not isinstance(other, KafkaSource):
            raise TypeError(
                "Comparisons should only involve KafkaSource class objects."
            )

        if not super().__eq__(other):
            return False

        if (
            self.kafka_options.kafka_bootstrap_servers
            != other.kafka_options.kafka_bootstrap_servers
            or self.kafka_options.message_format != other.kafka_options.message_format
            or self.kafka_options.topic != other.kafka_options.topic
            or self.kafka_options.watermark_delay_threshold
            != other.kafka_options.watermark_delay_threshold
        ):
            return False

        return True

    def __hash__(self):
        return super().__hash__()

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        watermark_delay_threshold = None
        if data_source.kafka_options.watermark_delay_threshold:
            watermark_delay_threshold = (
                timedelta(days=0)
                if data_source.kafka_options.watermark_delay_threshold.ToNanoseconds()
                == 0
                else data_source.kafka_options.watermark_delay_threshold.ToTimedelta()
            )
        return KafkaSource(
            name=data_source.name,
            field_mapping=dict(data_source.field_mapping),
            kafka_bootstrap_servers=data_source.kafka_options.kafka_bootstrap_servers,
            message_format=StreamFormat.from_proto(
                data_source.kafka_options.message_format
            ),
            watermark_delay_threshold=watermark_delay_threshold,
            topic=data_source.kafka_options.topic,
            created_timestamp_column=data_source.created_timestamp_column,
            timestamp_field=data_source.timestamp_field,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
            batch_source=(
                DataSource.from_proto(data_source.batch_source)
                if data_source.batch_source
                else None
            ),
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.STREAM_KAFKA,
            field_mapping=self.field_mapping,
            kafka_options=self.kafka_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        if self.batch_source:
            data_source_proto.batch_source.MergeFrom(self.batch_source.to_proto())

        return data_source_proto

    def validate(self, config: RepoConfig):
        raise NotImplementedError

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        raise NotImplementedError

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.redshift_to_feast_value_type

    def get_table_query_string(self) -> str:
        raise NotImplementedError

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.STREAM_KAFKA


@typechecked
class RequestSource(DataSource):
    """
    RequestSource that can be used to provide input features for on demand transforms

    Attributes:
        name: Name of the request data source
        schema: Schema mapping from the input feature name to a ValueType
        description: A human-readable description.
        tags: A dictionary of key-value pairs to store arbitrary metadata.
        owner: The owner of the request data source, typically the email of the primary
            maintainer.
    """

    name: str
    schema: List[Field]
    description: str
    tags: Dict[str, str]
    owner: str

    def __init__(
        self,
        *,
        name: str,
        schema: List[Field],
        timestamp_field: Optional[str] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        """Creates a RequestSource object."""
        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            description=description,
            tags=tags,
            owner=owner,
        )
        self.schema = schema

    def validate(self, config: RepoConfig):
        raise NotImplementedError

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        raise NotImplementedError

    def __eq__(self, other):
        if not isinstance(other, RequestSource):
            raise TypeError(
                "Comparisons should only involve RequestSource class objects."
            )

        if not super().__eq__(other):
            return False

        if isinstance(self.schema, List) and isinstance(other.schema, List):
            for field1, field2 in zip(self.schema, other.schema):
                if field1 != field2:
                    return False
            return True
        else:
            return False

    def __hash__(self):
        return super().__hash__()

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        schema_pb = data_source.request_data_options.schema
        list_schema = []
        for field_proto in schema_pb:
            list_schema.append(Field.from_proto(field_proto))

        return RequestSource(
            name=data_source.name,
            schema=list_schema,
            timestamp_field=data_source.timestamp_field,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        schema_pb = []

        if isinstance(self.schema, Dict):
            for key, value in self.schema.items():
                schema_pb.append(
                    Field(name=key, dtype=from_value_type(value.value)).to_proto()
                )
        else:
            for field in self.schema:
                schema_pb.append(field.to_proto())
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.REQUEST_SOURCE,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )
        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.request_data_options.schema.extend(schema_pb)

        return data_source_proto

    def get_table_query_string(self) -> str:
        raise NotImplementedError

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        raise NotImplementedError

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.REQUEST_SOURCE


@typechecked
class KinesisSource(DataSource):
    """A KinesisSource allows users to register Kinesis streams as data sources."""

    def validate(self, config: RepoConfig):
        raise NotImplementedError

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        raise NotImplementedError

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        return KinesisSource(
            name=data_source.name,
            timestamp_field=data_source.timestamp_field,
            field_mapping=dict(data_source.field_mapping),
            record_format=StreamFormat.from_proto(
                data_source.kinesis_options.record_format
            ),
            region=data_source.kinesis_options.region,
            stream_name=data_source.kinesis_options.stream_name,
            created_timestamp_column=data_source.created_timestamp_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
            batch_source=(
                DataSource.from_proto(data_source.batch_source)
                if data_source.batch_source
                else None
            ),
        )

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        raise NotImplementedError

    def get_table_query_string(self) -> str:
        raise NotImplementedError

    def __init__(
        self,
        *,
        name: str,
        record_format: StreamFormat,
        region: str,
        stream_name: str,
        timestamp_field: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        batch_source: Optional[DataSource] = None,
    ):
        """
        Args:
            name: The unique name of the Kinesis source.
            record_format: The record format of the Kinesis stream.
            region: The AWS region of the Kinesis stream.
            stream_name: The name of the Kinesis stream.
            timestamp_field: Event timestamp field used for point-in-time joins of
            feature values.
            created_timestamp_column:  Timestamp column indicating when the row
            was created, used for deduplicating rows.
            field_mapping: A dictionary mapping of column names in this data
            source to feature names in a feature table or view. Only used for feature
            columns, not entity or timestamp columns.
            description: A human-readable description.
            tags: A dictionary of key-value pairs to store arbitrary metadata.
            owner: The owner of the Kinesis source, typically the email of the primary
            maintainer.
            batch_source: A DataSource backing the Kinesis stream (used for retrieving historical features).
        """
        if record_format is None:
            raise ValueError("Record format must be specified for kinesis source")

        super().__init__(
            name=name,
            timestamp_field=timestamp_field,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            description=description,
            tags=tags,
            owner=owner,
        )
        self.batch_source = batch_source
        self.kinesis_options = KinesisOptions(
            record_format=record_format, region=region, stream_name=stream_name
        )

    def __eq__(self, other):
        if not isinstance(other, KinesisSource):
            raise TypeError(
                "Comparisons should only involve KinesisSource class objects."
            )

        if not super().__eq__(other):
            return False

        if (
            self.kinesis_options.record_format != other.kinesis_options.record_format
            or self.kinesis_options.region != other.kinesis_options.region
            or self.kinesis_options.stream_name != other.kinesis_options.stream_name
        ):
            return False

        return True

    def __hash__(self):
        return super().__hash__()

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.STREAM_KINESIS,
            field_mapping=self.field_mapping,
            kinesis_options=self.kinesis_options.to_proto(),
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        data_source_proto.timestamp_field = self.timestamp_field
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        if self.batch_source:
            data_source_proto.batch_source.MergeFrom(self.batch_source.to_proto())

        return data_source_proto

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.STREAM_KINESIS


class PushMode(enum.Enum):
    ONLINE = 1
    OFFLINE = 2
    ONLINE_AND_OFFLINE = 3


@typechecked
class PushSource(DataSource):
    """
    A source that can be used to ingest features on request
    """

    # TODO(adchia): consider adding schema here in case where Feast manages pushing events to the offline store
    # TODO(adchia): consider a "mode" to support pushing raw vs transformed events
    batch_source: Optional[DataSource] = None

    def __init__(
        self,
        *,
        name: str,
        batch_source: Optional[DataSource] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        """
        Creates a PushSource object.

        Args:
            name: Name of the push source
            batch_source: The batch source that backs this push source. It's used when materializing from the offline
                store to the online store, and when retrieving historical features.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the data source, typically the email of the primary
                maintainer.
        """
        super().__init__(name=name, description=description, tags=tags, owner=owner)
        self.batch_source = batch_source

    def __eq__(self, other):
        if not isinstance(other, PushSource):
            return False

        if not super().__eq__(other):
            return False

        if self.batch_source != other.batch_source:
            return False

        return True

    def __hash__(self):
        return super().__hash__()

    def validate(self, config: RepoConfig):
        raise NotImplementedError

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        raise NotImplementedError

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        batch_source = (
            DataSource.from_proto(data_source.batch_source)
            if data_source.HasField("batch_source")
            else None
        )

        return PushSource(
            name=data_source.name,
            batch_source=batch_source,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def _to_proto_impl(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.PUSH_SOURCE,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
        )

        # Only set timestamp fields if we have a batch source and this PushSource doesn't have its own fields
        if self.batch_source and not (
            self.timestamp_field or self.created_timestamp_column or self.field_mapping
        ):
            data_source_proto.timestamp_field = self.batch_source.timestamp_field
            data_source_proto.created_timestamp_column = (
                self.batch_source.created_timestamp_column
            )
            data_source_proto.field_mapping.update(self.batch_source.field_mapping)
            data_source_proto.date_partition_column = (
                self.batch_source.date_partition_column
            )

        if self.batch_source:
            data_source_proto.batch_source.MergeFrom(self.batch_source.to_proto())

        return data_source_proto

    def get_table_query_string(self) -> str:
        raise NotImplementedError

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        raise NotImplementedError

    def source_type(self) -> DataSourceProto.SourceType.ValueType:
        return DataSourceProto.PUSH_SOURCE
