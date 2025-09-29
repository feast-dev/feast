# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from .DataSource_p2p import DataSource
# from .DataSource_p2p import BigQueryOptions
# from .DataSource_p2p import CustomSourceOptions
# from .DataSource_p2p import FileOptions
# from .DataSource_p2p import RedshiftOptions
# from .DataSource_p2p import SnowflakeOptions
# from .DataSource_p2p import SparkOptions
# from .DataSource_p2p import TrinoOptions
from datetime import datetime
from google.protobuf.message import Message  # type: ignore
from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator
import typing


class SavedDatasetStorage(BaseModel):
    _one_of_dict = {"SavedDatasetStorage.kind": {"fields": {"athena_storage", "bigquery_storage", "custom_storage", "file_storage", "redshift_storage", "snowflake_storage", "spark_storage", "trino_storage"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
    file_storage: typing.Optional[DataSource.FileOptions] = Field(default=None)
    bigquery_storage: typing.Optional[DataSource.BigQueryOptions] = Field(default=None)
    redshift_storage: typing.Optional[DataSource.RedshiftOptions] = Field(default=None)
    snowflake_storage: typing.Optional[DataSource.SnowflakeOptions] = Field(default=None)
    trino_storage: typing.Optional[DataSource.TrinoOptions] = Field(default=None)
    spark_storage: typing.Optional[DataSource.SparkOptions] = Field(default=None)
    custom_storage: typing.Optional[DataSource.CustomSourceOptions] = Field(default=None)
    athena_storage: typing.Optional[DataSource.AthenaOptions] = Field(default=None)

class SavedDatasetSpec(BaseModel):
# Name of the dataset. Must be unique since it's possible to overwrite dataset by name
    name: str = Field(default="")
# Name of Feast project that this Dataset belongs to.
    project: str = Field(default="")
# list of feature references with format "<view name>:<feature name>"
    features: typing.List[str] = Field(default_factory=list)
# entity columns + request columns from all feature views used during retrieval
    joinKeys: typing.List[str] = Field(default_factory=list)
# Whether full feature names are used in stored data
    full_feature_names: bool = Field(default=False)
    storage: typing.Optional[SavedDatasetStorage] = Field(default=None)
# Optional and only populated if generated from a feature service fetch
    feature_service_name: str = Field(default="")
# User defined metadata
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)

class SavedDatasetMeta(BaseModel):
# Time when this saved dataset is created
    created_timestamp: datetime = Field(default_factory=datetime.now)
# Time when this saved dataset is last updated
    last_updated_timestamp: datetime = Field(default_factory=datetime.now)
# Min timestamp in the dataset (needed for retrieval)
    min_event_timestamp: datetime = Field(default_factory=datetime.now)
# Max timestamp in the dataset (needed for retrieval)
    max_event_timestamp: datetime = Field(default_factory=datetime.now)

class SavedDataset(BaseModel):
    spec: SavedDatasetSpec = Field(default_factory=SavedDatasetSpec)
    meta: SavedDatasetMeta = Field(default_factory=SavedDatasetMeta)
