# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from ..types.Value_p2p import ValueType
from .DataFormat_p2p import FileFormat
from .DataFormat_p2p import StreamFormat
from .Feature_p2p import FeatureSpecV2
from datetime import datetime
from datetime import timedelta
from enum import IntEnum
from pydantic import field_validator
from google.protobuf.message import Message  # type: ignore
from protobuf_to_pydantic.customer_validator import check_one_of
from protobuf_to_pydantic.util import Timedelta
from pydantic import BaseModel
from pydantic import BeforeValidator
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator
from typing_extensions import Annotated
import typing


class DataSource(BaseModel):
    """
     Defines a Data Source that can be used source Feature data
 Next available id: 28
    """
    class SourceMeta(BaseModel):
        earliestEventTimestamp: datetime = Field(default_factory=datetime.now)
        latestEventTimestamp: datetime = Field(default_factory=datetime.now)
        created_timestamp: datetime = Field(default_factory=datetime.now)
        last_updated_timestamp: datetime = Field(default_factory=datetime.now)

    class FileOptions(BaseModel):
        """
         Defines options for DataSource that sources features from a file
        """

        file_format: FileFormat = Field(default_factory=FileFormat)
# Target URL of file to retrieve and source features from.
# s3://path/to/file for AWS S3 storage
# gs://path/to/file for GCP GCS storage
# file:///path/to/file for local storage
        uri: str = Field(default="")
# override AWS S3 storage endpoint with custom S3 endpoint
        s3_endpoint_override: str = Field(default="")

    class BigQueryOptions(BaseModel):
        """
         Defines options for DataSource that sources features from a BigQuery Query
        """

# Full table reference in the form of [project:dataset.table]
        table: str = Field(default="")
# SQL query that returns a table containing feature data. Must contain an event_timestamp column, and respective
# entity columns
        query: str = Field(default="")

    class TrinoOptions(BaseModel):
        """
         Defines options for DataSource that sources features from a Trino Query
        """

# Full table reference in the form of [project:dataset.table]
        table: str = Field(default="")
# SQL query that returns a table containing feature data. Must contain an event_timestamp column, and respective
# entity columns
        query: str = Field(default="")

    class KafkaOptions(BaseModel):
        """
         Defines options for DataSource that sources features from Kafka messages.
 Each message should be a Protobuf that can be decoded with the generated
 Java Protobuf class at the given class path
        """

# Comma separated list of Kafka bootstrap servers. Used for feature tables without a defined source host[:port]]
        kafka_bootstrap_servers: str = Field(default="")
# Kafka topic to collect feature data from.
        topic: str = Field(default="")
# Defines the stream data format encoding feature/entity data in Kafka messages.
        message_format: StreamFormat = Field(default_factory=StreamFormat)
# Watermark delay threshold for stream data
        watermark_delay_threshold: Annotated[timedelta, BeforeValidator(Timedelta.validate)] = Field(default_factory=timedelta)

    class KinesisOptions(BaseModel):
        """
         Defines options for DataSource that sources features from Kinesis records.
 Each record should be a Protobuf that can be decoded with the generated
 Java Protobuf class at the given class path
        """

# AWS region of the Kinesis stream
        region: str = Field(default="")
# Name of the Kinesis stream to obtain feature data from.
        stream_name: str = Field(default="")
# Defines the data format encoding the feature/entity data in Kinesis records.
# Kinesis Data Sources support Avro and Proto as data formats.
        record_format: StreamFormat = Field(default_factory=StreamFormat)

    class RedshiftOptions(BaseModel):
        """
         Defines options for DataSource that sources features from a Redshift Query
        """

# Redshift table name
        table: str = Field(default="")
# SQL query that returns a table containing feature data. Must contain an event_timestamp column, and respective
# entity columns
        query: str = Field(default="")
# Redshift schema name
        schema: str = Field(default="")
# Redshift database name
        database: str = Field(default="")

    class AthenaOptions(BaseModel):
        """
         Defines options for DataSource that sources features from a Athena Query
        """

# Athena table name
        table: str = Field(default="")
# SQL query that returns a table containing feature data. Must contain an event_timestamp column, and respective
# entity columns
        query: str = Field(default="")
# Athena database name
        database: str = Field(default="")
# Athena schema name
        data_source: str = Field(default="")

    class SnowflakeOptions(BaseModel):
        """
         Defines options for DataSource that sources features from a Snowflake Query
        """

# Snowflake table name
        table: str = Field(default="")
# SQL query that returns a table containing feature data. Must contain an event_timestamp column, and respective
# entity columns
        query: str = Field(default="")
# Snowflake schema name
        schema: str = Field(default="")
# Snowflake schema name
        database: str = Field(default="")

    class SparkOptions(BaseModel):
        """
         Defines options for DataSource that sources features from a spark table/query
        """

# Table name
        table: str = Field(default="")
# Spark SQl query that returns the table, this is an alternative to `table`
        query: str = Field(default="")
# Path from which spark can read the table, this is an alternative to `table`
        path: str = Field(default="")
# Format of files at `path` (e.g. parquet, avro, etc)
        file_format: str = Field(default="")
# Date Format of date partition column (e.g. %Y-%m-%d)
        date_partition_column_format: str = Field(default="")

    class CustomSourceOptions(BaseModel):
        """
         Defines configuration for custom third-party data sources.
        """

# Serialized configuration information for the data source. The implementer of the custom data source is
# responsible for serializing and deserializing data from bytes
        configuration: bytes = Field(default=b"")

    class RequestDataOptions(BaseModel):
        """
         Defines options for DataSource that sources features from request data
        """

# Mapping of feature name to type
        deprecated_schema: "typing.Dict[str, ValueType.Enum]" = Field(default_factory=dict)
        schema: typing.List[FeatureSpecV2] = Field(default_factory=list)

    class PushOptions(BaseModel):
        """
         Defines options for DataSource that supports pushing data to it. This allows data to be pushed to
 the online store on-demand, such as by stream consumers.
        """

    class SourceType(IntEnum):
        """
         Type of Data Source.
 Next available id: 12
        """
        INVALID = 0
        BATCH_FILE = 1
        BATCH_SNOWFLAKE = 8
        BATCH_BIGQUERY = 2
        BATCH_REDSHIFT = 5
        STREAM_KAFKA = 3
        STREAM_KINESIS = 4
        CUSTOM_SOURCE = 6
        REQUEST_SOURCE = 7
        PUSH_SOURCE = 9
        BATCH_TRINO = 10
        BATCH_SPARK = 11
        BATCH_ATHENA = 12

    _one_of_dict = {"DataSource.options": {"fields": {"athena_options", "bigquery_options", "custom_options", "file_options", "kafka_options", "kinesis_options", "push_options", "redshift_options", "request_data_options", "snowflake_options", "spark_options", "trino_options"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
    model_config = ConfigDict(validate_default=True)
# Unique name of data source within the project
    name: str = Field(default="")
# Name of Feast project that this data source belongs to.
    project: str = Field(default="")
    description: str = Field(default="")
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    owner: str = Field(default="")
    type: "DataSource.SourceType" = Field(default=0)
    
    @field_validator('type', mode='before')
    @classmethod
    def validate_type(cls, v):
        if isinstance(v, str):
            # Convert string enum names to integer values
            type_mapping = {
                'INVALID': 0,
                'BATCH_FILE': 1,
                'BATCH_BIGQUERY': 2,
                'STREAM_KAFKA': 3,
                'STREAM_KINESIS': 4,
                'BATCH_REDSHIFT': 5,
                'CUSTOM_SOURCE': 6,
                'REQUEST_SOURCE': 7,
                'BATCH_SNOWFLAKE': 8,
                'PUSH_SOURCE': 9,
                'BATCH_TRINO': 10,
                'BATCH_SPARK': 11,
                'BATCH_ATHENA': 12,
            }
            return type_mapping.get(v, 0)
        return v
# Defines mapping between fields in the sourced data
# and fields in parent FeatureTable.
    field_mapping: "typing.Dict[str, str]" = Field(default_factory=dict)
# Must specify event timestamp column name
    timestamp_field: str = Field(default="")
# (Optional) Specify partition column
# useful for file sources
    date_partition_column: str = Field(default="")
# Must specify creation timestamp column name
    created_timestamp_column: str = Field(default="")
# This is an internal field that is represents the python class for the data source object a proto object represents.
# This should be set by feast, and not by users.
# The field is used primarily by custom data sources and is mandatory for them to set. Feast may set it for
# first party sources as well.
    data_source_class_type: str = Field(default="")
# Optional batch source for streaming sources for historical features and materialization.
    batch_source: typing.Optional["DataSource"] = Field(default=None)
    meta: "DataSource.SourceMeta" = Field(default_factory=lambda : DataSource.SourceMeta())
    file_options: "DataSource.FileOptions" = Field(default_factory=lambda : DataSource.FileOptions())
    bigquery_options: "DataSource.BigQueryOptions" = Field(default_factory=lambda : DataSource.BigQueryOptions())
    kafka_options: "DataSource.KafkaOptions" = Field(default_factory=lambda : DataSource.KafkaOptions())
    kinesis_options: "DataSource.KinesisOptions" = Field(default_factory=lambda : DataSource.KinesisOptions())
    redshift_options: "DataSource.RedshiftOptions" = Field(default_factory=lambda : DataSource.RedshiftOptions())
    request_data_options: "DataSource.RequestDataOptions" = Field(default_factory=lambda : DataSource.RequestDataOptions())
    custom_options: "DataSource.CustomSourceOptions" = Field(default_factory=lambda : DataSource.CustomSourceOptions())
    snowflake_options: "DataSource.SnowflakeOptions" = Field(default_factory=lambda : DataSource.SnowflakeOptions())
    push_options: "DataSource.PushOptions" = Field(default_factory=lambda : DataSource.PushOptions())
    spark_options: "DataSource.SparkOptions" = Field(default_factory=lambda : DataSource.SparkOptions())
    trino_options: "DataSource.TrinoOptions" = Field(default_factory=lambda : DataSource.TrinoOptions())
    athena_options: "DataSource.AthenaOptions" = Field(default_factory=lambda : DataSource.AthenaOptions())

class DataSourceList(BaseModel):
    datasources: typing.List[DataSource] = Field(default_factory=list)
