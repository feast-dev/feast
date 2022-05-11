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
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

from google.protobuf.json_format import MessageToJson

from feast import type_map
from feast.data_format import StreamFormat
from feast.field import Field
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig, get_data_source_class_from_type
from feast.types import VALUE_TYPES_TO_FEAST_TYPES
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
    BATCH_TRINO = 5


class KafkaOptions:
    """
    DataSource Kafka options used to source features from Kafka messages
    """

    def __init__(
        self, bootstrap_servers: str, message_format: StreamFormat, topic: str,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.message_format = message_format
        self.topic = topic

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
    DataSourceProto.SourceType.STREAM_KAFKA: "feast.data_source.KafkaSource",
    DataSourceProto.SourceType.STREAM_KINESIS: "feast.data_source.KinesisSource",
    DataSourceProto.SourceType.REQUEST_SOURCE: "feast.data_source.RequestSource",
    DataSourceProto.SourceType.PUSH_SOURCE: "feast.data_source.PushSource",
}


class DataSource(ABC):
    """
    DataSource that can be used to source features.

    Args:
        name: Name of data source, which should be unique within a project
        timestamp_field (optional): (Deprecated) Event timestamp column used for point in time
            joins of feature values.
        created_timestamp_column (optional): Timestamp column indicating when the row
            was created, used for deduplicating rows.
        field_mapping (optional): A dictionary mapping of column names in this data
            source to feature names in a feature table or view. Only used for feature
            columns, not entity or timestamp columns.
        date_partition_column (optional): Timestamp column used for partitioning.
        description (optional) A human-readable description.
        tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
        owner (optional): The owner of the data source, typically the email of the primary
            maintainer.
        timestamp_field (optional): Event timestamp field used for point in time
            joins of feature values.
    """

    name: str
    timestamp_field: str
    created_timestamp_column: str
    field_mapping: Dict[str, str]
    date_partition_column: str
    description: str
    tags: Dict[str, str]
    owner: str

    def __init__(
        self,
        *,
        event_timestamp_column: Optional[str] = None,
        created_timestamp_column: Optional[str] = None,
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        name: Optional[str] = None,
        timestamp_field: Optional[str] = None,
    ):
        """
        Creates a DataSource object.
        Args:
            name: Name of data source, which should be unique within a project
            event_timestamp_column (optional): (Deprecated) Event timestamp column used for point in time
                joins of feature values.
            created_timestamp_column (optional): Timestamp column indicating when the row
                was created, used for deduplicating rows.
            field_mapping (optional): A dictionary mapping of column names in this data
                source to feature names in a feature table or view. Only used for feature
                columns, not entity or timestamp columns.
            date_partition_column (optional): Timestamp column used for partitioning.
            description (optional): A human-readable description.
            tags (optional): A dictionary of key-value pairs to store arbitrary metadata.
            owner (optional): The owner of the data source, typically the email of the primary
                maintainer.
            timestamp_field (optional): Event timestamp field used for point
                in time joins of feature values.
        """
        if not name:
            warnings.warn(
                (
                    "Names for data sources need to be supplied. "
                    "Data sources without names will not be supported after Feast 0.23."
                ),
                UserWarning,
            )
        self.name = name or ""
        if not timestamp_field and event_timestamp_column:
            warnings.warn(
                (
                    "The argument 'event_timestamp_column' is being deprecated. Please use 'timestamp_field' instead. "
                    "instead. Feast 0.23 and onwards will not support the argument 'event_timestamp_column' for datasources."
                ),
                DeprecationWarning,
            )
        self.timestamp_field = timestamp_field or event_timestamp_column or ""
        self.created_timestamp_column = (
            created_timestamp_column if created_timestamp_column else ""
        )
        self.field_mapping = field_mapping if field_mapping else {}
        self.date_partition_column = (
            date_partition_column if date_partition_column else ""
        )
        self.description = description or ""
        self.tags = tags or {}
        self.owner = owner or ""

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
            return cls.from_proto(data_source)

        cls = get_data_source_class_from_type(_DATA_SOURCE_OPTIONS[data_source_type])
        return cls.from_proto(data_source)

    @abstractmethod
    def to_proto(self) -> DataSourceProto:
        """
        Converts a DataSourceProto object to its protobuf representation.
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
        *args,
        name: Optional[str] = None,
        event_timestamp_column: Optional[str] = "",
        bootstrap_servers: Optional[str] = None,
        message_format: Optional[StreamFormat] = None,
        topic: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        timestamp_field: Optional[str] = "",
        batch_source: Optional[DataSource] = None,
    ):
        positional_attributes = [
            "name",
            "event_timestamp_column",
            "bootstrap_servers",
            "message_format",
            "topic",
        ]
        _name = name
        _event_timestamp_column = event_timestamp_column
        _bootstrap_servers = bootstrap_servers or ""
        _message_format = message_format
        _topic = topic or ""

        if args:
            warnings.warn(
                (
                    "Kafka parameters should be specified as a keyword argument instead of a positional arg."
                    "Feast 0.23+ will not support positional arguments to construct Kafka sources"
                ),
                DeprecationWarning,
            )
            if len(args) > len(positional_attributes):
                raise ValueError(
                    f"Only {', '.join(positional_attributes)} are allowed as positional args when defining "
                    f"Kafka sources, for backwards compatibility."
                )
            if len(args) >= 1:
                _name = args[0]
            if len(args) >= 2:
                _event_timestamp_column = args[1]
            if len(args) >= 3:
                _bootstrap_servers = args[2]
            if len(args) >= 4:
                _message_format = args[3]
            if len(args) >= 5:
                _topic = args[4]

        if _message_format is None:
            raise ValueError("Message format must be specified for Kafka source")

        super().__init__(
            event_timestamp_column=_event_timestamp_column,
            created_timestamp_column=created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
            description=description,
            tags=tags,
            owner=owner,
            name=_name,
            timestamp_field=timestamp_field,
        )
        self.batch_source = batch_source
        self.kafka_options = KafkaOptions(
            bootstrap_servers=_bootstrap_servers,
            message_format=_message_format,
            topic=_topic,
        )

    def __eq__(self, other):
        if not isinstance(other, KafkaSource):
            raise TypeError(
                "Comparisons should only involve KafkaSource class objects."
            )

        if not super().__eq__(other):
            return False

        if (
            self.kafka_options.bootstrap_servers
            != other.kafka_options.bootstrap_servers
            or self.kafka_options.message_format != other.kafka_options.message_format
            or self.kafka_options.topic != other.kafka_options.topic
        ):
            return False

        return True

    def __hash__(self):
        return super().__hash__()

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        return KafkaSource(
            name=data_source.name,
            event_timestamp_column=data_source.timestamp_field,
            field_mapping=dict(data_source.field_mapping),
            bootstrap_servers=data_source.kafka_options.bootstrap_servers,
            message_format=StreamFormat.from_proto(
                data_source.kafka_options.message_format
            ),
            topic=data_source.kafka_options.topic,
            created_timestamp_column=data_source.created_timestamp_column,
            timestamp_field=data_source.timestamp_field,
            date_partition_column=data_source.date_partition_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
            batch_source=DataSource.from_proto(data_source.batch_source)
            if data_source.batch_source
            else None,
        )

    def to_proto(self) -> DataSourceProto:
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
        data_source_proto.date_partition_column = self.date_partition_column
        if self.batch_source:
            data_source_proto.batch_source.MergeFrom(self.batch_source.to_proto())
        return data_source_proto

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.redshift_to_feast_value_type

    def get_table_query_string(self) -> str:
        raise NotImplementedError


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
        *args,
        name: Optional[str] = None,
        schema: Optional[Union[Dict[str, ValueType], List[Field]]] = None,
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
    ):
        """Creates a RequestSource object."""
        positional_attributes = ["name", "schema"]
        _name = name
        _schema = schema
        if args:
            warnings.warn(
                (
                    "Request source parameters should be specified as a keyword argument instead of a positional arg."
                    "Feast 0.23+ will not support positional arguments to construct request sources"
                ),
                DeprecationWarning,
            )
            if len(args) > len(positional_attributes):
                raise ValueError(
                    f"Only {', '.join(positional_attributes)} are allowed as positional args when defining "
                    f"feature views, for backwards compatibility."
                )
            if len(args) >= 1:
                _name = args[0]
            if len(args) >= 2:
                _schema = args[1]

        super().__init__(name=_name, description=description, tags=tags, owner=owner)
        if not _schema:
            raise ValueError("Schema needs to be provided for Request Source")
        if isinstance(_schema, Dict):
            warnings.warn(
                "Schema in RequestSource is changing type. The schema data type Dict[str, ValueType] is being deprecated in Feast 0.23. "
                "Please use List[Field] instead for the schema",
                DeprecationWarning,
            )
            schemaList = []
            for key, valueType in _schema.items():
                schemaList.append(
                    Field(name=key, dtype=VALUE_TYPES_TO_FEAST_TYPES[valueType])
                )
            self.schema = schemaList
        elif isinstance(_schema, List):
            self.schema = _schema
        else:
            raise Exception(
                "Schema type must be either dictionary or list, not "
                + str(type(_schema))
            )

    def validate(self, config: RepoConfig):
        pass

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        pass

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

        deprecated_schema = data_source.request_data_options.deprecated_schema
        schema_pb = data_source.request_data_options.schema

        if deprecated_schema and not schema_pb:
            warnings.warn(
                "Schema in RequestSource is changing type. The schema data type Dict[str, ValueType] is being deprecated in Feast 0.23. "
                "Please use List[Field] instead for the schema",
                DeprecationWarning,
            )
            dict_schema = {}
            for key, val in deprecated_schema.items():
                dict_schema[key] = ValueType(val)
            return RequestSource(
                name=data_source.name,
                schema=dict_schema,
                description=data_source.description,
                tags=dict(data_source.tags),
                owner=data_source.owner,
            )
        else:
            list_schema = []
            for field_proto in schema_pb:
                list_schema.append(Field.from_proto(field_proto))

            return RequestSource(
                name=data_source.name,
                schema=list_schema,
                description=data_source.description,
                tags=dict(data_source.tags),
                owner=data_source.owner,
            )

    def to_proto(self) -> DataSourceProto:

        schema_pb = []

        if isinstance(self.schema, Dict):
            for key, value in self.schema.items():
                schema_pb.append(
                    Field(
                        name=key, dtype=VALUE_TYPES_TO_FEAST_TYPES[value.value]
                    ).to_proto()
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
        data_source_proto.request_data_options.schema.extend(schema_pb)

        return data_source_proto

    def get_table_query_string(self) -> str:
        raise NotImplementedError

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        raise NotImplementedError


class RequestDataSource(RequestSource):
    def __init__(self, *args, **kwargs):
        warnings.warn(
            "The 'RequestDataSource' class is deprecated and was renamed to RequestSource. Please use RequestSource instead. This class name will be removed in Feast 0.23.",
            DeprecationWarning,
        )
        super().__init__(*args, **kwargs)


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
            name=data_source.name,
            event_timestamp_column=data_source.timestamp_field,
            field_mapping=dict(data_source.field_mapping),
            record_format=StreamFormat.from_proto(
                data_source.kinesis_options.record_format
            ),
            region=data_source.kinesis_options.region,
            stream_name=data_source.kinesis_options.stream_name,
            created_timestamp_column=data_source.created_timestamp_column,
            timestamp_field=data_source.timestamp_field,
            date_partition_column=data_source.date_partition_column,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
            batch_source=DataSource.from_proto(data_source.batch_source)
            if data_source.batch_source
            else None,
        )

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        pass

    def get_table_query_string(self) -> str:
        raise NotImplementedError

    def __init__(
        self,
        *args,
        name: Optional[str] = None,
        event_timestamp_column: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        record_format: Optional[StreamFormat] = None,
        region: Optional[str] = "",
        stream_name: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        description: Optional[str] = "",
        tags: Optional[Dict[str, str]] = None,
        owner: Optional[str] = "",
        timestamp_field: Optional[str] = "",
        batch_source: Optional[DataSource] = None,
    ):
        positional_attributes = [
            "name",
            "event_timestamp_column",
            "created_timestamp_column",
            "record_format",
            "region",
            "stream_name",
        ]
        _name = name
        _event_timestamp_column = event_timestamp_column
        _created_timestamp_column = created_timestamp_column
        _record_format = record_format
        _region = region or ""
        _stream_name = stream_name or ""
        if args:
            warnings.warn(
                (
                    "Kinesis parameters should be specified as a keyword argument instead of a positional arg."
                    "Feast 0.23+ will not support positional arguments to construct kinesis sources"
                ),
                DeprecationWarning,
            )
            if len(args) > len(positional_attributes):
                raise ValueError(
                    f"Only {', '.join(positional_attributes)} are allowed as positional args when defining "
                    f"kinesis sources, for backwards compatibility."
                )
            if len(args) >= 1:
                _name = args[0]
            if len(args) >= 2:
                _event_timestamp_column = args[1]
            if len(args) >= 3:
                _created_timestamp_column = args[2]
            if len(args) >= 4:
                _record_format = args[3]
            if len(args) >= 5:
                _region = args[4]
            if len(args) >= 6:
                _stream_name = args[5]

        if _record_format is None:
            raise ValueError("Record format must be specified for kinesis source")

        super().__init__(
            name=_name,
            event_timestamp_column=_event_timestamp_column,
            created_timestamp_column=_created_timestamp_column,
            field_mapping=field_mapping,
            date_partition_column=date_partition_column,
            description=description,
            tags=tags,
            owner=owner,
            timestamp_field=timestamp_field,
        )
        self.batch_source = batch_source
        self.kinesis_options = KinesisOptions(
            record_format=_record_format, region=_region, stream_name=_stream_name
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

    def to_proto(self) -> DataSourceProto:
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
        data_source_proto.date_partition_column = self.date_partition_column
        if self.batch_source:
            data_source_proto.batch_source.MergeFrom(self.batch_source.to_proto())

        return data_source_proto


class PushSource(DataSource):
    """
    A source that can be used to ingest features on request
    """

    # TODO(adchia): consider adding schema here in case where Feast manages pushing events to the offline store
    # TODO(adchia): consider a "mode" to support pushing raw vs transformed events
    batch_source: DataSource

    def __init__(
        self,
        *args,
        name: Optional[str] = None,
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
        positional_attributes = ["name", "batch_source"]
        _name = name
        _batch_source = batch_source
        if args:
            warnings.warn(
                (
                    "Push source parameters should be specified as a keyword argument instead of a positional arg."
                    "Feast 0.23+ will not support positional arguments to construct push sources"
                ),
                DeprecationWarning,
            )
            if len(args) > len(positional_attributes):
                raise ValueError(
                    f"Only {', '.join(positional_attributes)} are allowed as positional args when defining "
                    f"push sources, for backwards compatibility."
                )
            if len(args) >= 1:
                _name = args[0]
            if len(args) >= 2:
                _batch_source = args[1]

        super().__init__(name=_name, description=description, tags=tags, owner=owner)
        if not _batch_source:
            raise ValueError(
                f"batch_source parameter is needed for push source {self.name}"
            )
        self.batch_source = _batch_source

    def __eq__(self, other):
        if not isinstance(other, PushSource):
            raise TypeError("Comparisons should only involve PushSource class objects.")

        if not super().__eq__(other):
            return False

        if self.batch_source != other.batch_source:
            return False

        return True

    def __hash__(self):
        return super().__hash__()

    def validate(self, config: RepoConfig):
        pass

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        pass

    @staticmethod
    def from_proto(data_source: DataSourceProto):
        assert data_source.HasField("batch_source")
        batch_source = DataSource.from_proto(data_source.batch_source)

        return PushSource(
            name=data_source.name,
            batch_source=batch_source,
            description=data_source.description,
            tags=dict(data_source.tags),
            owner=data_source.owner,
        )

    def to_proto(self) -> DataSourceProto:
        batch_source_proto = None
        if self.batch_source:
            batch_source_proto = self.batch_source.to_proto()

        data_source_proto = DataSourceProto(
            name=self.name,
            type=DataSourceProto.PUSH_SOURCE,
            description=self.description,
            tags=self.tags,
            owner=self.owner,
            batch_source=batch_source_proto,
        )

        return data_source_proto

    def get_table_query_string(self) -> str:
        raise NotImplementedError

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        raise NotImplementedError
