# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
import typing
from datetime import datetime
from enum import IntEnum

from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..types.Value_p2p import RepeatedValue, Value


class FieldStatus(IntEnum):
    INVALID = 0
    PRESENT = 1
    NULL_VALUE = 2
    NOT_FOUND = 3
    OUTSIDE_MAX_AGE = 4


class GetFeastServingInfoRequest(BaseModel):
    pass


class GetFeastServingInfoResponse(BaseModel):
    # Feast version of this serving deployment.
    version: str = Field(default="")


class FeatureReferenceV2(BaseModel):
    # Name of the Feature View to retrieve the feature from.
    feature_view_name: str = Field(default="")
    # Name of the Feature to retrieve the feature from.
    feature_name: str = Field(default="")


class GetOnlineFeaturesRequestV2(BaseModel):
    """
    ToDo (oleksii): remove this message (since it's not used) and move EntityRow on package level
    """

    class EntityRow(BaseModel):
        # Request timestamp of this row. This value will be used,
        # together with maxAge, to determine feature staleness.
        timestamp: datetime = Field(default_factory=datetime.now)
        # Map containing mapping of entity name to entity value.
        fields: "typing.Dict[str, Value]" = Field(default_factory=dict)

    # List of features that are being retrieved
    features: typing.List[FeatureReferenceV2] = Field(default_factory=list)
    # List of entity rows, containing entity id and timestamp data.
    # Used during retrieval of feature rows and for joining feature
    # rows into a final dataset
    entity_rows: typing.List["GetOnlineFeaturesRequestV2.EntityRow"] = Field(
        default_factory=list
    )
    # Optional field to specify project name override. If specified, uses the
    # given project for retrieval. Overrides the projects specified in
    # Feature References if both are specified.
    project: str = Field(default="")


class FeatureList(BaseModel):
    """
    In JSON "val" field can be omitted
    """

    val: typing.List[str] = Field(default_factory=list)


class GetOnlineFeaturesRequest(BaseModel):
    _one_of_dict = {
        "GetOnlineFeaturesRequest.kind": {"fields": {"feature_service", "features"}}
    }
    one_of_validator = model_validator(mode="before")(check_one_of)
    feature_service: str = Field(default="")
    features: FeatureList = Field(default_factory=FeatureList)
    # The entity data is specified in a columnar format
    # A map of entity name -> list of values
    entities: "typing.Dict[str, RepeatedValue]" = Field(default_factory=dict)
    full_feature_names: bool = Field(default=False)
    # Context for OnDemand Feature Transformation
    # (was moved to dedicated parameter to avoid unnecessary separation logic on serving side)
    # A map of variable name -> list of values
    request_context: "typing.Dict[str, RepeatedValue]" = Field(default_factory=dict)


class GetOnlineFeaturesResponseMetadata(BaseModel):
    feature_names: FeatureList = Field(default_factory=FeatureList)


class GetOnlineFeaturesResponse(BaseModel):
    class FeatureVector(BaseModel):
        model_config = ConfigDict(validate_default=True)
        values: typing.List[Value] = Field(default_factory=list)
        statuses: typing.List[FieldStatus] = Field(default_factory=list)
        event_timestamps: typing.List[datetime] = Field(default_factory=list)

    metadata: GetOnlineFeaturesResponseMetadata = Field(
        default_factory=GetOnlineFeaturesResponseMetadata
    )
    # Length of "results" array should match length of requested features.
    # We also preserve the same order of features here as in metadata.feature_names
    results: typing.List["GetOnlineFeaturesResponse.FeatureVector"] = Field(
        default_factory=list
    )
    status: bool = Field(default=False)
