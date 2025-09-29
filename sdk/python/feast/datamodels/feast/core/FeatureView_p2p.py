# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from .DataSource_p2p import DataSource
from .Feature_p2p import FeatureSpecV2
from datetime import datetime
from datetime import timedelta
from google.protobuf.message import Message  # type: ignore
from protobuf_to_pydantic.util import Timedelta
from pydantic import BaseModel
from pydantic import BeforeValidator
from pydantic import Field
from typing_extensions import Annotated
import typing


class FeatureViewSpec(BaseModel):
    """
     Next available id: 13
 TODO(adchia): refactor common fields from this and ODFV into separate metadata proto
    """

# Name of the feature view. Must be unique. Not updated.
    name: str = Field(default="")
# Name of Feast project that this feature view belongs to.
    project: str = Field(default="")
# List of names of entities associated with this feature view.
    entities: typing.List[str] = Field(default_factory=list)
# List of specifications for each feature defined as part of this feature view.
    features: typing.List[FeatureSpecV2] = Field(default_factory=list)
# List of specifications for each entity defined as part of this feature view.
    entity_columns: typing.List[FeatureSpecV2] = Field(default_factory=list)
# Description of the feature view.
    description: str = Field(default="")
# User defined metadata
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
# Owner of the feature view.
    owner: str = Field(default="")
# Features in this feature view can only be retrieved from online serving
# younger than ttl. Ttl is measured as the duration of time between
# the feature's event timestamp and when the feature is retrieved
# Feature values outside ttl will be returned as unset values and indicated to end user
    ttl: Annotated[timedelta, BeforeValidator(Timedelta.validate)] = Field(default_factory=timedelta)
# Batch/Offline DataSource where this view can retrieve offline feature data.
    batch_source: typing.Optional[DataSource] = Field(default=None)
# Streaming DataSource from where this view can consume "online" feature data.
    stream_source: typing.Optional[DataSource] = Field(default=None)
# Whether these features should be served online or not
# This is also used to determine whether the features should be written to the online store
    online: bool = Field(default=False)
# Whether these features should be written to the offline store
    offline: bool = Field(default=False)
    source_views: typing.List["FeatureViewSpec"] = Field(default_factory=list)

class MaterializationInterval(BaseModel):
    start_time: datetime = Field(default_factory=datetime.now)
    end_time: datetime = Field(default_factory=datetime.now)

class FeatureViewMeta(BaseModel):
# Time where this Feature View is created
    created_timestamp: datetime = Field(default_factory=datetime.now)
# Time where this Feature View is last updated
    last_updated_timestamp: datetime = Field(default_factory=datetime.now)
# List of pairs (start_time, end_time) for which this feature view has been materialized.
    materialization_intervals: typing.List[MaterializationInterval] = Field(default_factory=list)

class FeatureView(BaseModel):
# User-specified specifications of this feature view.
    spec: FeatureViewSpec = Field(default_factory=FeatureViewSpec)
# System-populated metadata for this feature view.
    meta: FeatureViewMeta = Field(default_factory=FeatureViewMeta)

class FeatureViewList(BaseModel):
    featureviews: typing.List[FeatureView] = Field(default_factory=list)
