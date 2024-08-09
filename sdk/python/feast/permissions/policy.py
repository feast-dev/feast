from abc import ABC, abstractmethod
from typing import Any

from feast.permissions.user import User
from feast.protos.feast.core.Policy_pb2 import Policy as PolicyProto
from feast.protos.feast.core.Policy_pb2 import RoleBasedPolicy as RoleBasedPolicyProto


class Policy(ABC):
    """
    An abstract class to ensure that the current user matches the configured security policies.
    """

    @abstractmethod
    def validate_user(self, user: User) -> tuple[bool, str]:
        """
        Validate the given user against the configured policy.

        Args:
            user: The current user.

        Returns:
            bool: `True` if the user matches the policy criteria, `False` otherwise.
            str: A possibly empty explanation of the reason for not matching the configured policy.
        """
        raise NotImplementedError

    @staticmethod
    def from_proto(policy_proto: PolicyProto) -> Any:
        """
        Converts policy config in protobuf spec to a Policy class object.

        Args:
            policy_proto: A protobuf representation of a Policy.

        Returns:
            A Policy class object.
        """
        policy_type = policy_proto.WhichOneof("policy_type")
        if policy_type == "role_based_policy":
            return RoleBasedPolicy.from_proto(policy_proto)
        if policy_type is None:
            return None
        raise NotImplementedError(f"policy_type is unsupported: {policy_type}")

    @abstractmethod
    def to_proto(self) -> PolicyProto:
        """
        Converts a PolicyProto object to its protobuf representation.
        """
        raise NotImplementedError


class RoleBasedPolicy(Policy):
    """
    A `Policy` implementation where the user roles must be enforced to grant access to the requested action.
    At least one of the configured roles must be granted to the current user in order to allow the execution of the secured operation.

    E.g., if the policy enforces roles `a` and `b`, the user must have at least one of them in order to satisfy the policy.
    """

    def __init__(
        self,
        roles: list[str],
    ):
        self.roles = roles

    def __eq__(self, other):
        if not isinstance(other, RoleBasedPolicy):
            raise TypeError(
                "Comparisons should only involve RoleBasedPolicy class objects."
            )

        if sorted(self.roles) != sorted(other.roles):
            return False

        return True

    def get_roles(self) -> list[str]:
        return self.roles

    def validate_user(self, user: User) -> tuple[bool, str]:
        """
        Validate the given `user` against the configured roles.
        """
        result = user.has_matching_role(self.roles)
        explain = "" if result else f"Requires roles {self.roles}"
        return (result, explain)

    @staticmethod
    def from_proto(policy_proto: PolicyProto) -> Any:
        """
        Converts policy config in protobuf spec to a Policy class object.

        Args:
            policy_proto: A protobuf representation of a Policy.

        Returns:
            A RoleBasedPolicy class object.
        """
        return RoleBasedPolicy(roles=list(policy_proto.role_based_policy.roles))

    def to_proto(self) -> PolicyProto:
        """
        Converts a PolicyProto object to its protobuf representation.
        """

        role_based_policy_proto = RoleBasedPolicyProto(roles=self.roles)
        policy_proto = PolicyProto(role_based_policy=role_based_policy_proto)

        return policy_proto


def allow_all(self, user: User) -> tuple[bool, str]:
    return True, ""


def empty_policy(self) -> PolicyProto:
    return PolicyProto()


"""
A `Policy` instance to allow execution of any action to each user
"""
AllowAll = type(
    "AllowAll",
    (Policy,),
    {Policy.validate_user.__name__: allow_all, Policy.to_proto.__name__: empty_policy},
)()
