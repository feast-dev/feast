# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
import typing
from datetime import datetime, timedelta

from protobuf_to_pydantic.util import Timedelta
from pydantic import BaseModel, BeforeValidator, Field
from typing_extensions import Annotated

from .DataSource_p2p import DataSource
from .Feature_p2p import FeatureSpecV2


class FeatureTableSpec(BaseModel):
    # Name of the feature table. Must be unique. Not updated.
    name: str = Field(default="")
    # Name of Feast project that this feature table belongs to.
    project: str = Field(default="")
    # List names of entities to associate with the Features defined in this
    # Feature Table. Not updatable.
    entities: typing.List[str] = Field(default_factory=list)
    # List of features specifications for each feature defined with this feature table.
    features: typing.List[FeatureSpecV2] = Field(default_factory=list)
    # User defined metadata
    labels: "typing.Dict[str, str]" = Field(default_factory=dict)
    # Features in this feature table can only be retrieved from online serving
    # younger than max age. Age is measured as the duration of time between
    # the feature's event timestamp and when the feature is retrieved
    # Feature values outside max age will be returned as unset values and indicated to end user
    max_age: Annotated[timedelta, BeforeValidator(Timedelta.validate)] = Field(
        default_factory=timedelta
    )
    # Batch/Offline DataSource to source batch/offline feature data.
    # Only batch DataSource can be specified
    # (ie source type should start with 'BATCH_')
    batch_source: typing.Optional[DataSource] = Field(default=None)
    # Stream/Online DataSource to source stream/online feature data.
    # Only stream DataSource can be specified
    # (ie source type should start with 'STREAM_')
    stream_source: typing.Optional[DataSource] = Field(default=None)


class FeatureTableMeta(BaseModel):
    # Time where this Feature Table is created
    created_timestamp: datetime = Field(default_factory=datetime.now)
    # Time where this Feature Table is last updated
    last_updated_timestamp: datetime = Field(default_factory=datetime.now)
    # Auto incrementing revision no. of this Feature Table
    revision: int = Field(default=0)
    # Hash entities, features, batch_source and stream_source to inform JobService if
    # jobs should be restarted should hash change
    hash: str = Field(default="")


class FeatureTable(BaseModel):
    # User-specified specifications of this feature table.
    spec: FeatureTableSpec = Field(default_factory=FeatureTableSpec)
    # System-populated metadata for this feature table.
    meta: FeatureTableMeta = Field(default_factory=FeatureTableMeta)
