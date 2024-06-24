import logging
from typing import Union

from feast.permissions.permission import AuthzedAction, is_of_expected_type
from feast.permissions.security_manager import get_security_manager

logger = logging.getLogger(__name__)


def require_permissions(actions: Union[list[AuthzedAction], AuthzedAction]):
    """
    A decorator to define the actions that are executed from within the current class method and that must be protected
    against unauthorized access.

    The first parameter of the protected method must be `self`
    """

    def require_permissions_decorator(func):
        def permission_checker(*args, **kwargs):
            logger.debug(f"permission_checker for {args}, {kwargs}")
            resource = args[0]
            if not is_of_expected_type(resource):
                raise NotImplementedError(
                    f"The first argument is not of a managed type but {type(resource)}"
                )

            sm = get_security_manager()
            if sm is None:
                return True

            sm.assert_permissions(
                resource=resource,
                actions=actions,
            )
            logger.debug(
                f"User {sm.current_user} can invoke {actions} on {resource.name}:{type(resource)} "
            )
            result = func(*args, **kwargs)
            return result

        return permission_checker

    return require_permissions_decorator
