# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from ..types.EntityKey_p2p import EntityKey
from ..types.Value_p2p import Value
from .ServingService_p2p import FeatureReferenceV2
from datetime import datetime
from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel
from pydantic import Field
import typing


class ConnectorFeature(BaseModel):
    reference: FeatureReferenceV2 = Field(default_factory=FeatureReferenceV2)
    timestamp: datetime = Field(default_factory=datetime.now)
    value: Value = Field(default_factory=Value)

class ConnectorFeatureList(BaseModel):
    featureList: typing.List[ConnectorFeature] = Field(default_factory=list)

class OnlineReadRequest(BaseModel):
    entityKeys: typing.List[EntityKey] = Field(default_factory=list)
    view: str = Field(default="")
    features: typing.List[str] = Field(default_factory=list)

class OnlineReadResponse(BaseModel):
    results: typing.List[ConnectorFeatureList] = Field(default_factory=list)
