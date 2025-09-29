# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from .FeatureViewProjection_p2p import FeatureViewProjection
from datetime import datetime
from google.protobuf.message import Message  # type: ignore
from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator
import typing


class LoggingConfig(BaseModel):
    class FileDestination(BaseModel):
        path: str = Field(default="")
        s3_endpoint_override: str = Field(default="")
# column names to use for partitioning
        partition_by: typing.List[str] = Field(default_factory=list)

    class BigQueryDestination(BaseModel):
# Full table reference in the form of [project:dataset.table]
        table_ref: str = Field(default="")

    class RedshiftDestination(BaseModel):
# Destination table name. ClusterId and database will be taken from an offline store config
        table_name: str = Field(default="")

    class AthenaDestination(BaseModel):
# Destination table name. data_source and database will be taken from an offline store config
        table_name: str = Field(default="")

    class SnowflakeDestination(BaseModel):
# Destination table name. Schema and database will be taken from an offline store config
        table_name: str = Field(default="")

    class CustomDestination(BaseModel):
        kind: str = Field(default="")
        config: "typing.Dict[str, str]" = Field(default_factory=dict)

    class CouchbaseColumnarDestination(BaseModel):
# Destination database name
        database: str = Field(default="")
# Destination scope name
        scope: str = Field(default="")
# Destination collection name
        collection: str = Field(default="")

    _one_of_dict = {"LoggingConfig.destination": {"fields": {"athena_destination", "bigquery_destination", "couchbase_columnar_destination", "custom_destination", "file_destination", "redshift_destination", "snowflake_destination"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
    sample_rate: float = Field(default=0.0)
    file_destination: FileDestination = Field(default_factory=FileDestination)
    bigquery_destination: BigQueryDestination = Field(default_factory=BigQueryDestination)
    redshift_destination: RedshiftDestination = Field(default_factory=RedshiftDestination)
    snowflake_destination: SnowflakeDestination = Field(default_factory=SnowflakeDestination)
    custom_destination: CustomDestination = Field(default_factory=CustomDestination)
    athena_destination: AthenaDestination = Field(default_factory=AthenaDestination)
    couchbase_columnar_destination: CouchbaseColumnarDestination = Field(default_factory=CouchbaseColumnarDestination)

class FeatureServiceSpec(BaseModel):
# Name of the Feature Service. Must be unique. Not updated.
    name: str = Field(default="")
# Name of Feast project that this Feature Service belongs to.
    project: str = Field(default="")
# Represents a projection that's to be applied on top of the FeatureView.
# Contains data such as the features to use from a FeatureView.
    features: typing.List[FeatureViewProjection] = Field(default_factory=list)
# User defined metadata
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
# Description of the feature service.
    description: str = Field(default="")
# Owner of the feature service.
    owner: str = Field(default="")
# (optional) if provided logging will be enabled for this feature service.
    logging_config: LoggingConfig = Field(default_factory=LoggingConfig)

class FeatureServiceMeta(BaseModel):
# Time where this Feature Service is created
    created_timestamp: datetime = Field(default_factory=datetime.now)
# Time where this Feature Service is last updated
    last_updated_timestamp: datetime = Field(default_factory=datetime.now)

class FeatureService(BaseModel):
# User-specified specifications of this feature service.
    spec: FeatureServiceSpec = Field(default_factory=FeatureServiceSpec)
# System-populated metadata for this feature service.
    meta: FeatureServiceMeta = Field(default_factory=FeatureServiceMeta)

class FeatureServiceList(BaseModel):
    featureservices: typing.List[FeatureService] = Field(default_factory=list)
