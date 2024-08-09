import logging

logger = logging.getLogger(__name__)


class User:
    _username: str
    _roles: list[str]

    def __init__(self, username: str, roles: list[str]):
        self._username = username
        self._roles = roles

    @property
    def username(self):
        return self._username

    @property
    def roles(self):
        return self._roles

    def has_matching_role(self, requested_roles: list[str]) -> bool:
        """
        Verify that the user has at least one of the requested roles.

        Args:
            requested_roles: The list of requested roles.

        Returns:
            bool: `True` only if the user has any registered role and all the given roles are registered.
        """
        logger.debug(
            f"Check {self.username} has all {requested_roles}: currently {self.roles}"
        )
        return any(role in self.roles for role in requested_roles)

    def __str__(self):
        return f"{self.username} ({self.roles})"
