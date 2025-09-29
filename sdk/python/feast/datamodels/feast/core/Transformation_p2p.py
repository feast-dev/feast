# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel, Field, model_validator


class UserDefinedFunctionV2(BaseModel):
    """
    Serialized representation of python function.
    """

    # The function name
    name: str = Field(default="")
    # The python-syntax function body (serialized by dill)
    body: bytes = Field(default=b"")
    # The string representation of the udf
    body_text: str = Field(default="")


class SubstraitTransformationV2(BaseModel):
    substrait_plan: bytes = Field(default=b"")
    ibis_function: bytes = Field(default=b"")


class FeatureTransformationV2(BaseModel):
    """
    A feature transformation executed as a user-defined function
    """

    _one_of_dict = {
        "FeatureTransformationV2.transformation": {
            "fields": {"substrait_transformation", "user_defined_function"}
        }
    }
    one_of_validator = model_validator(mode="before")(check_one_of)
    user_defined_function: UserDefinedFunctionV2 = Field(
        default_factory=UserDefinedFunctionV2
    )
    substrait_transformation: SubstraitTransformationV2 = Field(
        default_factory=SubstraitTransformationV2
    )
