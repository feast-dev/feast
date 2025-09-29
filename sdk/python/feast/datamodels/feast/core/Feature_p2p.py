# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from ..types.Value_p2p import ValueType
from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
import typing


class FeatureSpecV2(BaseModel):
    model_config = ConfigDict(validate_default=True)
# Name of the feature. Not updatable.
    name: str = Field(default="")
# Value type of the feature. Not updatable.
    value_type: ValueType.Enum = Field(default=0)
# Tags for user defined metadata on a feature
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
# Description of the feature.
    description: str = Field(default="")
# Field indicating the vector will be indexed for vector similarity search
    vector_index: bool = Field(default=False)
# Metric used for vector similarity search.
    vector_search_metric: str = Field(default="")
# Field indicating the vector length
    vector_length: int = Field(default=0)
