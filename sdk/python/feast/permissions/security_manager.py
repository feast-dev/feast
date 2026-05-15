import logging
import os
from contextvars import ContextVar
from typing import Callable, List, Optional, Union

from feast.errors import FeastObjectNotFoundException
from feast.feast_object import FeastObject
from feast.infra.registry.base_registry import BaseRegistry
from feast.permissions.action import AuthzedAction
from feast.permissions.enforcer import enforce_policy
from feast.permissions.permission import Permission
from feast.permissions.user import User
from feast.project import Project

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

    def assert_permissions(
        self,
        resources: list[FeastObject],
        actions: Union[AuthzedAction, List[AuthzedAction]],
        filter_only: bool = False,
    ) -> list[FeastObject]:
        """
        Verify if the current user is authorized to execute the requested actions on the given resources.

        If no permissions are defined, the result is to deny the execution.

        Args:
            resources: The resources for which we need to enforce authorized permission.
            actions: The requested actions to be authorized.
            filter_only: If `True`, it removes unauthorized resources from the returned value, otherwise it raises a `FeastPermissionError` the
            first unauthorized resource. Defaults to `False`.

        Returns:
            list[FeastObject]: A filtered list of the permitted resources, possibly empty.

        Raises:
            FeastPermissionError: If the current user is not authorized to execute all the requested actions on the given resources.
        """
        return enforce_policy(
            permissions=self.permissions,
            user=self.current_user if self.current_user is not None else User("", []),
            resources=resources,
            actions=actions if isinstance(actions, list) else [actions],
            filter_only=filter_only,
        )


def assert_permissions_to_update(
    resource: FeastObject,
    getter: Union[
        Callable[[str, str, bool], FeastObject], Callable[[str, bool], FeastObject]
    ],
    project: str,
    allow_cache: bool = True,
) -> FeastObject:
    """
    Verify if the current user is authorized to create or update the given resource.
    If the resource already exists, the user must be granted permission to execute DESCRIBE and UPDATE actions.
    If the resource does not exist, the user must be granted permission to execute the CREATE action.

    If no permissions are defined, the result is to deny the execution.

    Args:
        resource: The resources for which we need to enforce authorized permission.
        getter: The getter function used to retrieve the existing resource instance by name.
        The signature must be `get_permission(self, name: str, project: str, allow_cache: bool)`
        project: The project nane used in the getter function.
        allow_cache: Whether to use cached data. Defaults to `True`.
    Returns:
        FeastObject: The original `resource`, if permitted.

    Raises:
        FeastPermissionError: If the current user is not authorized to execute all the requested actions on the given resource or on the existing one.
    """
    sm = get_security_manager()
    if not is_auth_necessary(sm):
        return resource

    actions = [AuthzedAction.DESCRIBE, AuthzedAction.UPDATE]
    try:
        if isinstance(resource, Project):
            existing_resource = getter(
                name=resource.name,
                allow_cache=allow_cache,
            )  # type: ignore[call-arg]
        else:
            existing_resource = getter(
                name=resource.name,
                project=project,
                allow_cache=allow_cache,
            )  # type: ignore[call-arg]
        assert_permissions(resource=existing_resource, actions=actions)
    except FeastObjectNotFoundException:
        actions = [AuthzedAction.CREATE]
    resource_to_update = assert_permissions(resource=resource, actions=actions)
    return resource_to_update


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
        FeastPermissionError: If the current user is not authorized to execute the requested actions on the given resources.
    """

    sm = get_security_manager()
    if not is_auth_necessary(sm):
        return resource
    return sm.assert_permissions(  # type: ignore[union-attr]
        resources=[resource], actions=actions, filter_only=False
    )[0]


def permitted_resources(
    resources: list[FeastObject],
    actions: Union[AuthzedAction, List[AuthzedAction]],
) -> list[FeastObject]:
    """
    A utility function to invoke the `assert_permissions` method on the global security manager.

    If no global `SecurityManager` is defined (NoAuthConfig), all resources are permitted.
    If a SecurityManager exists but no user context and actions are requested, deny access for security.
    If a SecurityManager exists but user is intra-communication, allow access.

    Args:
        resources: The resources for which we need to enforce authorized permission.
        actions: The requested actions to be authorized.
    Returns:
        list[FeastObject]]: A filtered list of the permitted resources, possibly empty.
    """

    sm = get_security_manager()
    if not is_auth_necessary(sm):
        # Check if this is NoAuthConfig (no security manager) vs missing user context vs intra-communication
        if sm is None:
            # NoAuthConfig: allow all resources
            logger.debug("NoAuthConfig enabled - allowing access to all resources")
            return resources
        elif sm.current_user is not None:
            # Intra-communication user: allow all resources
            logger.debug("Intra-communication user - allowing access to all resources")
            return resources
        else:
            # Security manager exists but no user context - deny access for security
            logger.warning(
                "Security manager exists but no user context - denying access to all resources"
            )
            return []
    return sm.assert_permissions(resources=resources, actions=actions, filter_only=True)  # type: ignore[union-attr]


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


def is_auth_necessary(sm: Optional[SecurityManager]) -> bool:
    intra_communication_base64 = os.getenv("INTRA_COMMUNICATION_BASE64")

    # If no security manager, no auth is necessary
    if sm is None:
        return False

    # If security manager exists but no user context, auth is necessary (security-first approach)
    if sm.current_user is None:
        return True

    # If user is intra-communication, no auth is necessary
    if sm.current_user.username == intra_communication_base64:
        return False

    # Otherwise, auth is necessary
    return True
