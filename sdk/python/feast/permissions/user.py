import logging
from typing import Optional

logger = logging.getLogger(__name__)


class User:
    _username: str
    _roles: Optional[list[str]]
    _groups: Optional[list[str]]
    _namespaces: Optional[list[str]]

    def __init__(
        self,
        username: str,
        roles: list[str] = [],
        groups: list[str] = [],
        namespaces: list[str] = [],
    ):
        self._username = username
        self._roles = roles
        self._groups = groups
        self._namespaces = namespaces

    @property
    def username(self):
        return self._username

    @property
    def roles(self):
        return self._roles

    @property
    def groups(self):
        return self._groups

    @property
    def namespaces(self):
        return self._namespaces

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

    def has_matching_group(self, requested_groups: list[str]) -> bool:
        """
        Verify that the user has at least one of the requested groups.

        Args:
            requested_groups: The list of requested groups.

        Returns:
            bool: `True` only if the user has any registered group and all the given groups are registered.
        """
        logger.debug(
            f"Check {self.username} has all {requested_groups}: currently {self.groups}"
        )
        return any(group in self.groups for group in requested_groups)

    def has_matching_namespace(self, requested_namespaces: list[str]) -> bool:
        """
        Verify that the user has at least one of the requested namespaces.

        Args:
            requested_namespaces: The list of requested namespaces.

        Returns:
            bool: `True` only if the user has any registered namespace and all the given namespaces are registered.
        """
        logger.debug(
            f"Check {self.username} has all {requested_namespaces}: currently {self.namespaces}"
        )
        return any(namespace in self.namespaces for namespace in requested_namespaces)

    def __str__(self):
        return f"{self.username} (roles: {self.roles}, groups: {self.groups}, namespaces: {self.namespaces})"
