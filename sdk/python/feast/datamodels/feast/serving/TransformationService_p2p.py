# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
from enum import IntEnum

from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel, ConfigDict, Field, model_validator


class TransformationServiceType(IntEnum):
    TRANSFORMATION_SERVICE_TYPE_INVALID = 0
    TRANSFORMATION_SERVICE_TYPE_PYTHON = 1
    TRANSFORMATION_SERVICE_TYPE_CUSTOM = 100


class ValueType(BaseModel):
    _one_of_dict = {"ValueType.value": {"fields": {"arrow_value"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
    # Having a oneOf provides forward compatibility if we need to support compound types
    # that are not supported by arrow natively.
    arrow_value: bytes = Field(default=b"")


class GetTransformationServiceInfoRequest(BaseModel):
    pass


class GetTransformationServiceInfoResponse(BaseModel):
    model_config = ConfigDict(validate_default=True)
    # Feast version of this transformation service deployment.
    version: str = Field(default="")
    # Type of transformation service deployment. This is either Python, or custom
    type: TransformationServiceType = Field(default=0)
    transformation_service_type_details: str = Field(default="")


class TransformFeaturesRequest(BaseModel):
    on_demand_feature_view_name: str = Field(default="")
    project: str = Field(default="")
    transformation_input: ValueType = Field(default_factory=ValueType)


class TransformFeaturesResponse(BaseModel):
    transformation_output: ValueType = Field(default_factory=ValueType)
