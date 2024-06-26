import logging
from contextvars import ContextVar
from typing import List, Optional, Union

from feast.feast_object import FeastObject
from feast.permissions.action import AuthzedAction
from feast.permissions.enforcer import enforce_policy
from feast.permissions.permission import Permission
from feast.permissions.role_manager import RoleManager

logger = logging.getLogger(__name__)


class SecurityManager:
    """
    The security manager holds references to the security components (role manager, policy enforces) and the configured permissions.
    It is accessed and defined using the global functions `_get_security_manager` and `_set_security_manager`
    """

    def __init__(
        self,
        role_manager: RoleManager,
        permissions: list[Permission] = [],
    ):
        self._role_manager: RoleManager = role_manager
        self._permissions: list[Permission] = permissions
        self._current_user: ContextVar[Optional[str]] = ContextVar(
            "current_user", default=None
        )

    def set_current_user(self, user: str):
        self._current_user.set(user)

    @property
    def role_manager(self) -> RoleManager:
        """
        Returns:
            RoleManager: the configured `RoleManager` instance.
        """
        return self._role_manager

    @property
    def current_user(self) -> Optional[str]:
        """
        Returns:
            str: the possibly empty ID of the current user. `contextvars` module is used to ensure that each concurrent request has its own
            individual user ID.
        """
        return self._current_user.get()

    @property
    def permissions(self) -> list[Permission]:
        """
        Returns:
            list[Permission]: the list of `Permission` configured in the Feast registry.
        """
        return self._permissions

    def assert_permissions(
        self,
        resources: Union[list[FeastObject], FeastObject],
        actions: Union[AuthzedAction, List[AuthzedAction]],
    ):
        """
        Verify if the current user is authorized ro execute the requested actions on the given resources.

        If no permissions are defined, the result is to allow the execution.

        Args:
            resources: The resources for which we need to enforce authorized permission.
            actions: The requested actions to be authorized.

        Raises:
            PermissionError: If the current user is not authorized to eecute all the requested actions on the given resources.
        """
        result, explain = enforce_policy(
            role_manager=self._role_manager,
            permissions=self._permissions,
            user=self.current_user if self.current_user is not None else "",
            resources=resources,
            actions=actions if isinstance(actions, list) else [actions],
        )
        if not result:
            raise PermissionError(explain)


def assert_permissions(
    resources: Union[list[FeastObject], FeastObject],
    actions: Union[AuthzedAction, List[AuthzedAction]],
):
    """
    A utility function to invoke the `assert_permissions` method on the global security manager.

    If no global `SecurityManager` is defined, the execution is permitted.
    """
    sm = get_security_manager()
    if sm is None:
        return
    return sm.assert_permissions(resources=resources, actions=actions)


"""
The possibly empty global instance of `SecurityManager`.
"""
_sm: Optional[SecurityManager] = None


def get_security_manager() -> Optional[SecurityManager]:
    """
    Return the global instance of `SecurityManager`.
    """
    global _sm
    return _sm


def set_security_manager(sm: SecurityManager):
    """
    Initialize the global instance of `SecurityManager`.
    """

    global _sm
    _sm = sm
