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
from typing import Callable, Dict, Iterable, Optional, Tuple

from pyarrow.parquet import ParquetFile
from tenacity import retry, retry_unless_exception_type, wait_exponential

from feast import type_map
from feast.data_format import FileFormat, StreamFormat
from feast.errors import (
    DataSourceNotFoundException,
    RedshiftCredentialsError,
    RedshiftQueryError,
)
from feast.protos.feast.core.DataSource_pb2 import DataSource as DataSourceProto
from feast.repo_config import RepoConfig
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


class FileOptions:
    """
    DataSource File options used to source features from a file

    args:
        file_format (FileFormat, optional): file source format eg. parquet
        file_url (str, optional): file source url eg. s3:// or local file
        s3_endpoint_override (str, optional): custom s3 endpoint (used only with s3 file_url)
    """

    def __init__(
        self,
        file_format: Optional[FileFormat],
        file_url: Optional[str],
        s3_endpoint_override: Optional[str],
    ):
        self._file_format = file_format
        self._file_url = file_url
        self._s3_endpoint_override = s3_endpoint_override

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

    @property
    def s3_endpoint_override(self):
        """
        Returns the s3 endpoint override
        """
        return self._s3_endpoint_override

    @s3_endpoint_override.setter
    def s3_endpoint_override(self, s3_endpoint_override):
        """
        Sets the s3 endpoint override
        """
        self._s3_endpoint_override = s3_endpoint_override

    @classmethod
    def from_proto(cls, file_options_proto: DataSourceProto.FileOptions):
        """
        Creates a FileOptions from a protobuf representation of a file option

        args:
            file_options_proto: a protobuf representation of a datasource

        Returns:
            Returns a FileOptions object based on the file_options protobuf
        """
        file_options = cls(
            file_format=FileFormat.from_proto(file_options_proto.file_format),
            file_url=file_options_proto.file_url,
            s3_endpoint_override=file_options_proto.s3_endpoint_override,
        )
        return file_options

    def to_proto(self) -> DataSourceProto.FileOptions:
        """
        Converts an FileOptionsProto object to its protobuf representation.

        Returns:
            FileOptionsProto protobuf
        """

        file_options_proto = DataSourceProto.FileOptions(
            file_format=(
                None if self.file_format is None else self.file_format.to_proto()
            ),
            file_url=self.file_url,
            s3_endpoint_override=self.s3_endpoint_override,
        )

        return file_options_proto


class BigQueryOptions:
    """
    DataSource BigQuery options used to source features from BigQuery query
    """

    def __init__(self, table_ref: Optional[str], query: Optional[str]):
        self._table_ref = table_ref
        self._query = query

    @property
    def query(self):
        """
        Returns the BigQuery SQL query referenced by this source
        """
        return self._query

    @query.setter
    def query(self, query):
        """
        Sets the BigQuery SQL query referenced by this source
        """
        self._query = query

    @property
    def table_ref(self):
        """
        Returns the table ref of this BQ table
        """
        return self._table_ref

    @table_ref.setter
    def table_ref(self, table_ref):
        """
        Sets the table ref of this BQ table
        """
        self._table_ref = table_ref

    @classmethod
    def from_proto(cls, bigquery_options_proto: DataSourceProto.BigQueryOptions):
        """
        Creates a BigQueryOptions from a protobuf representation of a BigQuery option

        Args:
            bigquery_options_proto: A protobuf representation of a DataSource

        Returns:
            Returns a BigQueryOptions object based on the bigquery_options protobuf
        """

        bigquery_options = cls(
            table_ref=bigquery_options_proto.table_ref,
            query=bigquery_options_proto.query,
        )

        return bigquery_options

    def to_proto(self) -> DataSourceProto.BigQueryOptions:
        """
        Converts an BigQueryOptionsProto object to its protobuf representation.

        Returns:
            BigQueryOptionsProto protobuf
        """

        bigquery_options_proto = DataSourceProto.BigQueryOptions(
            table_ref=self.table_ref, query=self.query,
        )

        return bigquery_options_proto


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


class DataSource:
    """
    DataSource that can be used source features
    """

    def __init__(
        self,
        event_timestamp_column: Optional[str] = "",
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
    ):
        self._event_timestamp_column = event_timestamp_column
        self._created_timestamp_column = created_timestamp_column
        self._field_mapping = field_mapping if field_mapping else {}
        self._date_partition_column = date_partition_column

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
    def field_mapping(self):
        """
        Returns the field mapping of this data source
        """
        return self._field_mapping

    @field_mapping.setter
    def field_mapping(self, field_mapping):
        """
        Sets the field mapping of this data source
        """
        self._field_mapping = field_mapping

    @property
    def event_timestamp_column(self):
        """
        Returns the event timestamp column of this data source
        """
        return self._event_timestamp_column

    @event_timestamp_column.setter
    def event_timestamp_column(self, event_timestamp_column):
        """
        Sets the event timestamp column of this data source
        """
        self._event_timestamp_column = event_timestamp_column

    @property
    def created_timestamp_column(self):
        """
        Returns the created timestamp column of this data source
        """
        return self._created_timestamp_column

    @created_timestamp_column.setter
    def created_timestamp_column(self, created_timestamp_column):
        """
        Sets the created timestamp column of this data source
        """
        self._created_timestamp_column = created_timestamp_column

    @property
    def date_partition_column(self):
        """
        Returns the date partition column of this data source
        """
        return self._date_partition_column

    @date_partition_column.setter
    def date_partition_column(self, date_partition_column):
        """
        Sets the date partition column of this data source
        """
        self._date_partition_column = date_partition_column

    @staticmethod
    def from_proto(data_source):
        """
        Convert data source config in FeatureTable spec to a DataSource class object.
        """

        if data_source.file_options.file_format and data_source.file_options.file_url:
            data_source_obj = FileSource(
                field_mapping=data_source.field_mapping,
                file_format=FileFormat.from_proto(data_source.file_options.file_format),
                path=data_source.file_options.file_url,
                event_timestamp_column=data_source.event_timestamp_column,
                created_timestamp_column=data_source.created_timestamp_column,
                date_partition_column=data_source.date_partition_column,
            )

            if data_source.file_options.s3_endpoint_override:
                data_source_obj.file_options.s3_endpoint_override = (
                    data_source.file_options.s3_endpoint_override
                )

        elif (
            data_source.bigquery_options.table_ref or data_source.bigquery_options.query
        ):
            data_source_obj = BigQuerySource(
                field_mapping=data_source.field_mapping,
                table_ref=data_source.bigquery_options.table_ref,
                event_timestamp_column=data_source.event_timestamp_column,
                created_timestamp_column=data_source.created_timestamp_column,
                date_partition_column=data_source.date_partition_column,
                query=data_source.bigquery_options.query,
            )
        elif data_source.redshift_options.table or data_source.redshift_options.query:
            data_source_obj = RedshiftSource(
                field_mapping=data_source.field_mapping,
                table=data_source.redshift_options.table,
                event_timestamp_column=data_source.event_timestamp_column,
                created_timestamp_column=data_source.created_timestamp_column,
                date_partition_column=data_source.date_partition_column,
                query=data_source.redshift_options.query,
            )
        elif (
            data_source.kafka_options.bootstrap_servers
            and data_source.kafka_options.topic
            and data_source.kafka_options.message_format
        ):
            data_source_obj = KafkaSource(
                field_mapping=data_source.field_mapping,
                bootstrap_servers=data_source.kafka_options.bootstrap_servers,
                message_format=StreamFormat.from_proto(
                    data_source.kafka_options.message_format
                ),
                topic=data_source.kafka_options.topic,
                event_timestamp_column=data_source.event_timestamp_column,
                created_timestamp_column=data_source.created_timestamp_column,
                date_partition_column=data_source.date_partition_column,
            )
        elif (
            data_source.kinesis_options.record_format
            and data_source.kinesis_options.region
            and data_source.kinesis_options.stream_name
        ):
            data_source_obj = KinesisSource(
                field_mapping=data_source.field_mapping,
                record_format=StreamFormat.from_proto(
                    data_source.kinesis_options.record_format
                ),
                region=data_source.kinesis_options.region,
                stream_name=data_source.kinesis_options.stream_name,
                event_timestamp_column=data_source.event_timestamp_column,
                created_timestamp_column=data_source.created_timestamp_column,
                date_partition_column=data_source.date_partition_column,
            )
        else:
            raise ValueError("Could not identify the source type being added")

        return data_source_obj

    def to_proto(self) -> DataSourceProto:
        """
        Converts an DataSourceProto object to its protobuf representation.
        """
        raise NotImplementedError

    def validate(self, config: RepoConfig):
        """
        Validates the underlying data source.
        """
        raise NotImplementedError

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        """
        Get the callable method that returns Feast type given the raw column type
        """
        raise NotImplementedError

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        """
        Get the list of column names and raw column types
        """
        raise NotImplementedError


class FileSource(DataSource):
    def __init__(
        self,
        event_timestamp_column: Optional[str] = "",
        file_url: Optional[str] = None,
        path: Optional[str] = None,
        file_format: FileFormat = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        s3_endpoint_override: Optional[str] = "",
    ):
        """Create a FileSource from a file containing feature data. Only Parquet format supported.

        Args:

            path: File path to file containing feature data. Must contain an event_timestamp column, entity columns and
                feature columns.
            event_timestamp_column: Event timestamp column used for point in time joins of feature values.
            created_timestamp_column (optional): Timestamp column when row was created, used for deduplicating rows.
            file_url: [Deprecated] Please see path
            file_format (optional): Explicitly set the file format. Allows Feast to bypass inferring the file format.
            s3_endpoint_override (optional): Overrides AWS S3 enpoint with custom S3 storage
            field_mapping: A dictionary mapping of column names in this data source to feature names in a feature table
                or view. Only used for feature columns, not entities or timestamp columns.

        Examples:
            >>> FileSource(path="/data/my_features.parquet", event_timestamp_column="datetime")
        """
        if path is None and file_url is None:
            raise ValueError(
                'No "path" argument provided. Please set "path" to the location of your file source.'
            )
        if file_url:
            from warnings import warn

            warn(
                'Argument "file_url" is being deprecated. Please use the "path" argument.'
            )
        else:
            file_url = path

        self._file_options = FileOptions(
            file_format=file_format,
            file_url=file_url,
            s3_endpoint_override=s3_endpoint_override,
        )

        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

    def __eq__(self, other):
        if not isinstance(other, FileSource):
            raise TypeError("Comparisons should only involve FileSource class objects.")

        return (
            self.file_options.file_url == other.file_options.file_url
            and self.file_options.s3_endpoint_override
            == other.file_options.s3_endpoint_override
            and self.file_options.file_format == other.file_options.file_format
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def file_options(self):
        """
        Returns the file options of this data source
        """
        return self._file_options

    @file_options.setter
    def file_options(self, file_options):
        """
        Sets the file options of this data source
        """
        self._file_options = file_options

    @property
    def path(self):
        """
        Returns the file path of this feature data source
        """
        return self._file_options.file_url

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_FILE,
            field_mapping=self.field_mapping,
            file_options=self.file_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        # TODO: validate a FileSource
        pass

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.pa_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        schema = ParquetFile(self.path).schema_arrow
        return zip(schema.names, map(str, schema.types))


class BigQuerySource(DataSource):
    def __init__(
        self,
        event_timestamp_column: Optional[str] = "",
        table_ref: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        query: Optional[str] = None,
    ):
        self._bigquery_options = BigQueryOptions(table_ref=table_ref, query=query)

        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

    def __eq__(self, other):
        if not isinstance(other, BigQuerySource):
            raise TypeError(
                "Comparisons should only involve BigQuerySource class objects."
            )

        return (
            self.bigquery_options.table_ref == other.bigquery_options.table_ref
            and self.bigquery_options.query == other.bigquery_options.query
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def table_ref(self):
        return self._bigquery_options.table_ref

    @property
    def query(self):
        return self._bigquery_options.query

    @property
    def bigquery_options(self):
        """
        Returns the bigquery options of this data source
        """
        return self._bigquery_options

    @bigquery_options.setter
    def bigquery_options(self, bigquery_options):
        """
        Sets the bigquery options of this data source
        """
        self._bigquery_options = bigquery_options

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_BIGQUERY,
            field_mapping=self.field_mapping,
            bigquery_options=self.bigquery_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        if not self.query:
            from google.api_core.exceptions import NotFound
            from google.cloud import bigquery

            client = bigquery.Client()
            try:
                client.get_table(self.table_ref)
            except NotFound:
                raise DataSourceNotFoundException(self.table_ref)

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table_ref:
            return f"`{self.table_ref}`"
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.bq_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        from google.cloud import bigquery

        client = bigquery.Client()
        if self.table_ref is not None:
            table_schema = client.get_table(self.table_ref).schema
            if not isinstance(table_schema[0], bigquery.schema.SchemaField):
                raise TypeError("Could not parse BigQuery table schema.")

            name_type_pairs = [(field.name, field.field_type) for field in table_schema]
        else:
            bq_columns_query = f"SELECT * FROM ({self.query}) LIMIT 1"
            queryRes = client.query(bq_columns_query).result()
            name_type_pairs = [
                (schema_field.name, schema_field.field_type)
                for schema_field in queryRes.schema
            ]

        return name_type_pairs


class KafkaSource(DataSource):
    def __init__(
        self,
        event_timestamp_column: str,
        bootstrap_servers: str,
        message_format: StreamFormat,
        topic: str,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = dict(),
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


class KinesisSource(DataSource):
    def __init__(
        self,
        event_timestamp_column: str,
        created_timestamp_column: str,
        record_format: StreamFormat,
        region: str,
        stream_name: str,
        field_mapping: Optional[Dict[str, str]] = dict(),
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


class RedshiftOptions:
    """
    DataSource Redshift options used to source features from Redshift query
    """

    def __init__(self, table: Optional[str], query: Optional[str]):
        self._table = table
        self._query = query

    @property
    def query(self):
        """
        Returns the Redshift SQL query referenced by this source
        """
        return self._query

    @query.setter
    def query(self, query):
        """
        Sets the Redshift SQL query referenced by this source
        """
        self._query = query

    @property
    def table(self):
        """
        Returns the table name of this Redshift table
        """
        return self._table

    @table.setter
    def table(self, table_name):
        """
        Sets the table ref of this Redshift table
        """
        self._table = table_name

    @classmethod
    def from_proto(cls, redshift_options_proto: DataSourceProto.RedshiftOptions):
        """
        Creates a RedshiftOptions from a protobuf representation of a Redshift option

        Args:
            redshift_options_proto: A protobuf representation of a DataSource

        Returns:
            Returns a RedshiftOptions object based on the redshift_options protobuf
        """

        redshift_options = cls(
            table=redshift_options_proto.table, query=redshift_options_proto.query,
        )

        return redshift_options

    def to_proto(self) -> DataSourceProto.RedshiftOptions:
        """
        Converts an RedshiftOptionsProto object to its protobuf representation.

        Returns:
            RedshiftOptionsProto protobuf
        """

        redshift_options_proto = DataSourceProto.RedshiftOptions(
            table=self.table, query=self.query,
        )

        return redshift_options_proto


class RedshiftSource(DataSource):
    def __init__(
        self,
        event_timestamp_column: Optional[str] = "",
        table: Optional[str] = None,
        created_timestamp_column: Optional[str] = "",
        field_mapping: Optional[Dict[str, str]] = None,
        date_partition_column: Optional[str] = "",
        query: Optional[str] = None,
    ):
        super().__init__(
            event_timestamp_column,
            created_timestamp_column,
            field_mapping,
            date_partition_column,
        )

        self._redshift_options = RedshiftOptions(table=table, query=query)

    def __eq__(self, other):
        if not isinstance(other, RedshiftSource):
            raise TypeError(
                "Comparisons should only involve RedshiftSource class objects."
            )

        return (
            self.redshift_options.table == other.redshift_options.table
            and self.redshift_options.query == other.redshift_options.query
            and self.event_timestamp_column == other.event_timestamp_column
            and self.created_timestamp_column == other.created_timestamp_column
            and self.field_mapping == other.field_mapping
        )

    @property
    def table(self):
        return self._redshift_options.table

    @property
    def query(self):
        return self._redshift_options.query

    @property
    def redshift_options(self):
        """
        Returns the Redshift options of this data source
        """
        return self._redshift_options

    @redshift_options.setter
    def redshift_options(self, _redshift_options):
        """
        Sets the Redshift options of this data source
        """
        self._redshift_options = _redshift_options

    def to_proto(self) -> DataSourceProto:
        data_source_proto = DataSourceProto(
            type=DataSourceProto.BATCH_REDSHIFT,
            field_mapping=self.field_mapping,
            redshift_options=self.redshift_options.to_proto(),
        )

        data_source_proto.event_timestamp_column = self.event_timestamp_column
        data_source_proto.created_timestamp_column = self.created_timestamp_column
        data_source_proto.date_partition_column = self.date_partition_column

        return data_source_proto

    def validate(self, config: RepoConfig):
        # As long as the query gets successfully executed, or the table exists,
        # the data source is validated. We don't need the results though.
        # TODO: uncomment this
        # self.get_table_column_names_and_types(config)
        print("Validate", self.get_table_column_names_and_types(config))

    def get_table_query_string(self) -> str:
        """Returns a string that can directly be used to reference this table in SQL"""
        if self.table:
            return f"`{self.table}`"
        else:
            return f"({self.query})"

    @staticmethod
    def source_datatype_to_feast_value_type() -> Callable[[str], ValueType]:
        return type_map.redshift_to_feast_value_type

    def get_table_column_names_and_types(
        self, config: RepoConfig
    ) -> Iterable[Tuple[str, str]]:
        import boto3
        from botocore.config import Config
        from botocore.exceptions import ClientError

        from feast.infra.offline_stores.redshift import RedshiftOfflineStoreConfig

        assert isinstance(config.offline_store, RedshiftOfflineStoreConfig)

        client = boto3.client(
            "redshift-data", config=Config(region_name=config.offline_store.region)
        )

        try:
            if self.table is not None:
                table = client.describe_table(
                    ClusterIdentifier=config.offline_store.cluster_id,
                    Database=config.offline_store.database,
                    DbUser=config.offline_store.user,
                    Table=self.table,
                )
                # The API returns valid JSON with empty column list when the table doesn't exist
                if len(table["ColumnList"]) == 0:
                    raise DataSourceNotFoundException(self.table)

                columns = table["ColumnList"]
            else:
                statement = client.execute_statement(
                    ClusterIdentifier=config.offline_store.cluster_id,
                    Database=config.offline_store.database,
                    DbUser=config.offline_store.user,
                    Sql=f"SELECT * FROM ({self.query}) LIMIT 1",
                )

                # Need to retry client.describe_statement(...) until the task is finished. We don't want to bombard
                # Redshift with queries, and neither do we want to wait for a long time on the initial call.
                # The solution is exponential backoff. The backoff starts with 0.1 seconds and doubles exponentially
                # until reaching 30 seconds, at which point the backoff is fixed.
                @retry(
                    wait=wait_exponential(multiplier=0.1, max=30),
                    retry=retry_unless_exception_type(RedshiftQueryError),
                )
                def wait_for_statement():
                    desc = client.describe_statement(Id=statement["Id"])
                    if desc["Status"] in ("SUBMITTED", "STARTED", "PICKED"):
                        raise Exception  # Retry
                    if desc["Status"] != "FINISHED":
                        raise RedshiftQueryError(desc)  # Don't retry. Raise exception.

                wait_for_statement()

                result = client.get_statement_result(Id=statement["Id"])

                columns = result["ColumnMetadata"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ValidationException":
                raise RedshiftCredentialsError() from e
            raise

        return [(column["name"], column["typeName"].upper()) for column in columns]
