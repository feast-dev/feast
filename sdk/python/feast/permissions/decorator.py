import logging
from typing import Union

from feast.permissions.action import AuthzedAction
from feast.permissions.matcher import is_a_feast_object
from feast.permissions.security_manager import assert_permissions

logger = logging.getLogger(__name__)


def require_permissions(actions: Union[list[AuthzedAction], AuthzedAction]):
    """
    A decorator to define the actions that are executed from the decorated class method and that must be protected
    against unauthorized access.

    The first parameter of the protected method must be `self`
    Args:
        actions: The list of actions that must be permitted to the current user.
    """

    def require_permissions_decorator(func):
        def permission_checker(*args, **kwargs):
            logger.debug(f"permission_checker for {args}, {kwargs}")
            resource = args[0]
            if not is_a_feast_object(resource):
                raise NotImplementedError(
                    f"The first argument is not of a managed type but {type(resource)}"
                )

            return assert_permissions(
                resource=resource,
                actions=actions,
            )
            logger.debug(
                f"Current User can invoke {actions} on {resource.name}:{type(resource)} "
            )
            result = func(*args, **kwargs)
            return result

        return permission_checker

    return require_permissions_decorator
