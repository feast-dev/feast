# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.3.3.1](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.8
# Pydantic Version: 2.10.6
import typing

from protobuf_to_pydantic.customer_validator import check_one_of
from pydantic import BaseModel, Field, model_validator


class RoleBasedPolicy(BaseModel):
    # List of roles in this policy.
    roles: typing.List[str] = Field(default_factory=list)


class GroupBasedPolicy(BaseModel):
    # List of groups in this policy.
    groups: typing.List[str] = Field(default_factory=list)


class NamespaceBasedPolicy(BaseModel):
    # List of namespaces in this policy.
    namespaces: typing.List[str] = Field(default_factory=list)


class CombinedGroupNamespacePolicy(BaseModel):
    # List of groups in this policy.
    groups: typing.List[str] = Field(default_factory=list)
    # List of namespaces in this policy.
    namespaces: typing.List[str] = Field(default_factory=list)


class Policy(BaseModel):
    _one_of_dict = {
        "Policy.policy_type": {
            "fields": {
                "combined_group_namespace_policy",
                "group_based_policy",
                "namespace_based_policy",
                "role_based_policy",
            }
        }
    }
    one_of_validator = model_validator(mode="before")(check_one_of)
    # Name of the policy.
    name: str = Field(default="")
    # Name of Feast project.
    project: str = Field(default="")
    role_based_policy: RoleBasedPolicy = Field(default_factory=RoleBasedPolicy)
    group_based_policy: GroupBasedPolicy = Field(default_factory=GroupBasedPolicy)
    namespace_based_policy: NamespaceBasedPolicy = Field(
        default_factory=NamespaceBasedPolicy
    )
    combined_group_namespace_policy: CombinedGroupNamespacePolicy = Field(
        default_factory=CombinedGroupNamespacePolicy
    )
