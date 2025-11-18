from abc import ABC, abstractmethod
from typing import Any

from feast.permissions.user import User
from feast.protos.feast.core.Policy_pb2 import (
    CombinedGroupNamespacePolicy as CombinedGroupNamespacePolicyProto,
)
from feast.protos.feast.core.Policy_pb2 import GroupBasedPolicy as GroupBasedPolicyProto
from feast.protos.feast.core.Policy_pb2 import (
    NamespaceBasedPolicy as NamespaceBasedPolicyProto,
)
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
        elif policy_type == "group_based_policy":
            return GroupBasedPolicy.from_proto(policy_proto)
        elif policy_type == "namespace_based_policy":
            return NamespaceBasedPolicy.from_proto(policy_proto)
        elif policy_type == "combined_group_namespace_policy":
            return CombinedGroupNamespacePolicy.from_proto(policy_proto)
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


class GroupBasedPolicy(Policy):
    """
    A `Policy` implementation where the user groups must be enforced to grant access to the requested action.
    At least one of the configured groups must be granted to the current user in order to allow the execution of the secured operation.

    E.g., if the policy enforces groups `a` and `b`, the user must be added in one of them in order to satisfy the policy.
    """

    def __init__(
        self,
        groups: list[str],
    ):
        self.groups = groups

    def __eq__(self, other):
        if not isinstance(other, GroupBasedPolicy):
            raise TypeError(
                "Comparisons should only involve GroupBasedPolicy class objects."
            )

        if sorted(self.groups) != sorted(other.groups):
            return False

        return True

    def get_groups(self) -> list[str]:
        return self.groups

    def validate_user(self, user: User) -> tuple[bool, str]:
        """
        Validate the given `user` against the configured groups.
        """
        result = user.has_matching_group(self.groups)
        explain = "User is not added into the permitted groups" if not result else ""
        return (result, explain)

    @staticmethod
    def from_proto(policy_proto: PolicyProto) -> Any:
        """
        Converts policy config in protobuf spec to a Policy class object.

        Args:
            policy_proto: A protobuf representation of a Policy.

        Returns:
            A GroupBasedPolicy class object.
        """
        return GroupBasedPolicy(groups=list(policy_proto.group_based_policy.groups))

    def to_proto(self) -> PolicyProto:
        """
        Converts a GroupBasedPolicy object to its protobuf representation.
        """
        group_based_policy_proto = GroupBasedPolicyProto(groups=self.groups)
        policy_proto = PolicyProto(group_based_policy=group_based_policy_proto)
        return policy_proto


class NamespaceBasedPolicy(Policy):
    """
    A `Policy` implementation where the user must be added to the namespaces must be enforced to grant access to the requested action.
    User must be added to at least one of the permitted namespaces in order to allow the execution of the secured operation.

    E.g., if the policy enforces namespaces `a` and `b`, the user must have at least one of them in order to satisfy the policy.
    """

    def __init__(
        self,
        namespaces: list[str],
    ):
        self.namespaces = namespaces

    def __eq__(self, other):
        if not isinstance(other, NamespaceBasedPolicy):
            raise TypeError(
                "Comparisons should only involve NamespaceBasedPolicy class objects."
            )

        if sorted(self.namespaces) != sorted(other.namespaces):
            return False

        return True

    def get_namespaces(self) -> list[str]:
        return self.namespaces

    def validate_user(self, user: User) -> tuple[bool, str]:
        """
        Validate the given `user` against the configured namespaces.
        """
        result = user.has_matching_namespace(self.namespaces)
        explain = (
            "User is not added into the permitted namespaces" if not result else ""
        )
        return (result, explain)

    @staticmethod
    def from_proto(policy_proto: PolicyProto) -> Any:
        """
        Converts policy config in protobuf spec to a Policy class object.

        Args:
            policy_proto: A protobuf representation of a Policy.

        Returns:
            A NamespaceBasedPolicy class object.
        """
        return NamespaceBasedPolicy(
            namespaces=list(policy_proto.namespace_based_policy.namespaces)
        )

    def to_proto(self) -> PolicyProto:
        """
        Converts a NamespaceBasedPolicy object to its protobuf representation.
        """
        namespace_based_policy_proto = NamespaceBasedPolicyProto(
            namespaces=self.namespaces
        )
        policy_proto = PolicyProto(namespace_based_policy=namespace_based_policy_proto)
        return policy_proto


class CombinedGroupNamespacePolicy(Policy):
    """
    A `Policy` implementation that combines group-based and namespace-based authorization.
    The user must be in at least one of the permitted groups OR namespaces to satisfy the policy.
    """

    def __init__(
        self,
        groups: list[str],
        namespaces: list[str],
    ):
        self.groups = groups
        self.namespaces = namespaces

    def __eq__(self, other):
        if not isinstance(other, CombinedGroupNamespacePolicy):
            raise TypeError(
                "Comparisons should only involve CombinedGroupNamespacePolicy class objects."
            )

        if sorted(self.groups) != sorted(other.groups) or sorted(
            self.namespaces
        ) != sorted(other.namespaces):
            return False

        return True

    def get_groups(self) -> list[str]:
        return self.groups

    def get_namespaces(self) -> list[str]:
        return self.namespaces

    def validate_user(self, user: User) -> tuple[bool, str]:
        """
        Validate the given `user` against the permitted groups and namespaces.
        User must be added to one of the matching group or namespace.
        """
        has_matching_group = user.has_matching_group(self.groups)
        has_matching_namespace = user.has_matching_namespace(self.namespaces)

        result = has_matching_group or has_matching_namespace

        if not result:
            explain = (
                "User must be in at least one of the permitted groups or namespaces"
            )
        else:
            explain = ""

        return (result, explain)

    @staticmethod
    def from_proto(policy_proto: PolicyProto) -> Any:
        """
        Converts policy config in protobuf spec to a Policy class object.

        Args:
            policy_proto: A protobuf representation of a Policy.

        Returns:
            A CombinedGroupNamespacePolicy class object.
        """
        return CombinedGroupNamespacePolicy(
            groups=list(policy_proto.combined_group_namespace_policy.groups),
            namespaces=list(policy_proto.combined_group_namespace_policy.namespaces),
        )

    def to_proto(self) -> PolicyProto:
        """
        Converts a CombinedGroupNamespacePolicy object to its protobuf representation.
        """
        combined_policy_proto = CombinedGroupNamespacePolicyProto(
            groups=self.groups, namespaces=self.namespaces
        )
        policy_proto = PolicyProto(
            combined_group_namespace_policy=combined_policy_proto
        )
        return policy_proto


"""
A `Policy` instance to allow execution of any action to each user
"""
AllowAll = type(
    "AllowAll",
    (Policy,),
    {Policy.validate_user.__name__: allow_all, Policy.to_proto.__name__: empty_policy},
)()
