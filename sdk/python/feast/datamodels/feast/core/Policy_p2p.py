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


class RoleBasedPolicy(BaseModel):
# List of roles in this policy.
    roles: typing.List[str] = Field(default_factory=list)

class Policy(BaseModel):
    _one_of_dict = {"Policy.policy_type": {"fields": {"role_based_policy"}}}
    one_of_validator = model_validator(mode="before")(check_one_of)
# Name of the policy.
    name: str = Field(default="")
# Name of Feast project.
    project: str = Field(default="")
    role_based_policy: RoleBasedPolicy = Field(default_factory=RoleBasedPolicy)
