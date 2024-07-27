import logging
from contextvars import ContextVar
from typing import List, Optional, Union

from feast.feast_object import FeastObject
from feast.infra.registry.base_registry import BaseRegistry
from feast.permissions.action import AuthzedAction
from feast.permissions.decision import DecisionStrategy
from feast.permissions.enforcer import enforce_policy
from feast.permissions.permission import Permission
from feast.permissions.user import User

logger = logging.getLogger(__name__)


class SecurityManager:
    """
    The security manager it's the entry point to validate the configuration of the current user against the configured permission policies.
    It is accessed and defined using the global functions `get_security_manager` and `set_security_manager`
    """

    def __init__(
        self,
        project: str,
        registry: BaseRegistry,
    ):
        self._project = project
        self._registry = registry
        self._current_user: ContextVar[Optional[User]] = ContextVar(
            "current_user", default=None
        )

    def set_current_user(self, current_user: User):
        """
        Init the user for the current context.
        """
        self._current_user.set(current_user)

    @property
    def current_user(self) -> Optional[User]:
        """
        Returns:
            str: the possibly empty instance of the current user. `contextvars` module is used to ensure that each concurrent request has its own
            individual user.
        """
        return self._current_user.get()

    @property
    def permissions(self) -> list[Permission]:
        """
        Returns:
            list[Permission]: the list of `Permission` configured in the Feast registry.
        """
        return self._registry.list_permissions(project=self._project)

    @property
    def decision_strategy(self) -> DecisionStrategy:
        """
        Returns:
            DecisionStrategy: the DecisionStrategy configured in the Feast registry.
        """
        return self._registry.list_project_metadata(project=self._project)[
            0
        ].decision_strategy

    def assert_permissions(
        self,
        resources: list[FeastObject],
        actions: Union[AuthzedAction, List[AuthzedAction]],
        filter_only: bool = False,
    ) -> list[FeastObject]:
        """
        Verify if the current user is authorized ro execute the requested actions on the given resources.

        If no permissions are defined, the result is to allow the execution.

        Args:
            resources: The resources for which we need to enforce authorized permission.
            actions: The requested actions to be authorized.
            filter_only: If `True`, it removes unauthorized resources from the returned value, otherwise it raises a `PermissionError` the
            first unauthorized resource. Defaults to `False`.

        Returns:
            list[FeastObject]: A filtered list of the permitted resources, possibly empty.

        Raises:
            PermissionError: If the current user is not authorized to eecute all the requested actions on the given resources.
        """
        return enforce_policy(
            permissions=self.permissions,
            user=self.current_user if self.current_user is not None else User("", []),
            resources=resources,
            actions=actions if isinstance(actions, list) else [actions],
            decision_strategy=self.decision_strategy,
            filter_only=filter_only,
        )


def assert_permissions(
    resource: FeastObject,
    actions: Union[AuthzedAction, List[AuthzedAction]],
) -> FeastObject:
    """
    A utility function to invoke the `assert_permissions` method on the global security manager.

    If no global `SecurityManager` is defined, the execution is permitted.

    Args:
        resource: The resource for which we need to enforce authorized permission.
        actions: The requested actions to be authorized.
    Returns:
        FeastObject: The original `resource`, if permitted.

    Raises:
        PermissionError: If the current user is not authorized to execute the requested actions on the given resources.
    """
    sm = get_security_manager()
    if sm is None:
        return resource
    return sm.assert_permissions(
        resources=[resource], actions=actions, filter_only=False
    )[0]


def permitted_resources(
    resources: list[FeastObject],
    actions: Union[AuthzedAction, List[AuthzedAction]],
) -> list[FeastObject]:
    """
    A utility function to invoke the `assert_permissions` method on the global security manager.

    If no global `SecurityManager` is defined, the execution is permitted.

    Args:
        resources: The resources for which we need to enforce authorized permission.
        actions: The requested actions to be authorized.
    Returns:
        list[FeastObject]]: A filtered list of the permitted resources, possibly empty.
    """
    sm = get_security_manager()
    if sm is None:
        return resources
    return sm.assert_permissions(resources=resources, actions=actions, filter_only=True)


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


def no_security_manager():
    """
    Initialize the empty global instance of `SecurityManager`.
    """

    global _sm
    _sm = None
