import logging
from typing import Union

from feast.feast_object import FeastObject
from feast.permissions.decision import DecisionEvaluator
from feast.permissions.permission import (
    AuthzedAction,
    Permission,
)
from feast.permissions.role_manager import RoleManager

logger = logging.getLogger(__name__)


def enforce_policy(
    role_manager: RoleManager,
    permissions: list[Permission],
    user: str,
    resources: Union[list[FeastObject], FeastObject],
    actions: list[AuthzedAction],
    filter_only: bool = False,
) -> list[FeastObject]:
    """
    Define the logic to apply the configured permissions when a given action is requested on
    a protected resource.

    If no permissions are defined, the result is to allow the execution.

    Args:
        role_manager: The `RoleManager` instance.
        permissions: The configured set of `Permission`.
        user: The current user.
        resources: The resources for which we need to enforce authorized permission.
        actions: The requested actions to be authorized.
        filter_only: If `True`, it removes unauthorized resources from the returned value, otherwise it raises a `PermissionError` the
        first unauthorized resource. Defaults to `False`.
    Returns:
        list[FeastObject]: A list of the permitted resources (a subset of the input `resources`).

    Raises:
        PermissionError: If the current user is not authorized to eecute the requested actions on the given resources (and `filter_only` is `False`).
    """
    _resources = resources if isinstance(resources, list) else [resources]
    if not permissions:
        return _resources

    _permitted_resources: list[FeastObject] = []
    for resource in _resources:
        logger.debug(
            f"Enforcing permission policies for {type(resource)}:{resource.name} to execute {actions}"
        )
        matching_permissions = [
            p
            for p in permissions
            if p.match_resource(resource) and p.match_actions(actions)
        ]

        if matching_permissions:
            evaluator = DecisionEvaluator(
                Permission.get_global_decision_strategy(), len(matching_permissions)
            )
            for p in matching_permissions:
                permission_grant, permission_explanation = p.policy.validate_user(
                    user=user, role_manager=role_manager
                )
                evaluator.add_grant(
                    permission_grant,
                    f"Permission {p.name} denied access: {permission_explanation}",
                )

                if evaluator.is_decided():
                    grant, explanations = evaluator.grant()
                    if not grant and not filter_only:
                        raise PermissionError(",".join(explanations))
                    if grant:
                        _permitted_resources.append(resource)
                    break
        else:
            _permitted_resources.append(resource)
            message = f"No permissions defined to manage {actions} on {type(resource)}/{resource.name}."
            logger.info(f"**PERMISSION GRANTED**: {message}")
    return _permitted_resources
