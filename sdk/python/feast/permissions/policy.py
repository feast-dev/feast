from abc import ABC, abstractmethod

from feast.permissions.role_manager import RoleManager


class Policy(ABC):
    """
    An abstract class to ensure that the current user matches the configured security policies.
    """

    @abstractmethod
    def validate_user(self, user: str, **kwargs) -> tuple[bool, str]:
        """
        Validate the given user against the configured policy.

        Args:
            user: The current user.
            kwargs: The list of keyword args to be passed to the actual implementation.

        Returns:
            bool: `True` if the user matches the policy criteria, `False` otherwise.
            str: A possibly empty explanation of the reason for not matching the configured policy.
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

    def get_roles(self) -> list[str]:
        return self.roles

    def validate_user(self, user: str, **kwargs) -> tuple[bool, str]:
        """
        Validate the given `user` against the configured roles.

        The `role_manager` keywork argument must be present in the `kwargs` optional key-value arguments.
        """
        if "role_manager" not in kwargs:
            raise ValueError("Missing keywork argument 'role_manager'")
        if not isinstance(kwargs["role_manager"], RoleManager):
            raise ValueError(
                f"The keywork argument 'role_manager' is not of the expected type {RoleManager.__name__}"
            )
        rm = kwargs.get("role_manager")
        if isinstance(rm, RoleManager):
            result = rm.user_has_matching_role(user, self.roles)
        explain = "" if result else f"Requires roles {self.roles}"
        return (result, explain)


def allow_all(self, user: str, **kwargs) -> tuple[bool, str]:
    return True, ""


"""
A `Policy` instance to allow execution of any action to each user
"""
AllowAll = type("AllowAll", (Policy,), {Policy.validate_user.__name__: allow_all})()
