import logging

logger = logging.getLogger(__name__)


class RoleManager:
    """
    `RoleManager` is the registry of user roles captured from the external user request and used by the
    `RoleBasedPolicy` policy.
    """

    def __init__(self):
        self.roles_by_user = {}

    def add_roles_for_user(self, user: str, roles: list[str]):
        """
        Add the given roles to the given user.

        Args:
            user: The user ID.
            roles: The list of associated roles.
        """
        self.roles_by_user.setdefault(user, []).extend(roles)

    def clear(self) -> None:
        """
        Clear all the registered roles.
        """
        self.roles_by_user.clear()

    def user_has_matching_role(self, user: str, roles: list[str]) -> bool:
        """
        Verify that the given user has at least one of the requested roles.

        Args:
            user: The user ID.
            roles: The list of requested roles.

        Returns:
            bool: `True` only if the given user has any registered role and all the given roles are registered.
        """
        logger.debug(
            f"Check {user} has all {roles}: currently {self.roles_by_user.get(user, [])}"
        )
        user_roles = self.roles_by_user.get(user, [])
        return any(role in user_roles for role in roles)
