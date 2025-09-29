# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from .DataSource_p2p import DataSource
from .Feature_p2p import FeatureSpecV2
from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel
from pydantic import Field
import typing


class FeatureViewProjection(BaseModel):
    """
     A projection to be applied on top of a FeatureView. 
 Contains the modifications to a FeatureView such as the features subset to use.
    """

# The feature view name
    feature_view_name: str = Field(default="")
# Alias for feature view name
    feature_view_name_alias: str = Field(default="")
# The features of the feature view that are a part of the feature reference.
    feature_columns: typing.List[FeatureSpecV2] = Field(default_factory=list)
# Map for entity join_key overrides of feature data entity join_key to entity data join_key
    join_key_map: "typing.Dict[str, str]" = Field(default_factory=dict)
    timestamp_field: str = Field(default="")
    date_partition_column: str = Field(default="")
    created_timestamp_column: str = Field(default="")
# Batch/Offline DataSource where this view can retrieve offline feature data.
    batch_source: typing.Optional[DataSource] = Field(default=None)
# Streaming DataSource from where this view can consume "online" feature data.
    stream_source: typing.Optional[DataSource] = Field(default=None)
