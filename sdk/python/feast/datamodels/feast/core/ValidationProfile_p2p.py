# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8 
# Pydantic Version: 2.10.6 
from google.protobuf.message import Message  # type: ignore
from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel
from pydantic import Field
from pydantic import model_validator
import typing


class GEValidationProfiler(BaseModel):
    class UserDefinedProfiler(BaseModel):
# The python-syntax function body (serialized by dill)
        body: bytes = Field(default=b"")

    profiler: "GEValidationProfiler.UserDefinedProfiler" = Field(default_factory=lambda : GEValidationProfiler.UserDefinedProfiler())

class GEValidationProfile(BaseModel):
# JSON-serialized ExpectationSuite object
    expectation_suite: bytes = Field(default=b"")

class ValidationReference(BaseModel):
    _one_of_dict = {"ValidationReference.cached_profile": {"fields": {"ge_profile"}}, "ValidationReference.profiler": {"fields": {"ge_profiler"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
# Unique name of validation reference within the project
    name: str = Field(default="")
# Name of saved dataset used as reference dataset
    reference_dataset_name: str = Field(default="")
# Name of Feast project that this object source belongs to
    project: str = Field(default="")
# Description of the validation reference
    description: str = Field(default="")
# User defined metadata
    tags: "typing.Dict[str, str]" = Field(default_factory=dict)
    ge_profiler: GEValidationProfiler = Field(default_factory=GEValidationProfiler)
    ge_profile: GEValidationProfile = Field(default_factory=GEValidationProfile)
