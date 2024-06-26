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
) -> tuple[bool, str]:
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
    Returns:
        tuple[bool, str]: a boolean with the result of the authorization check (`True` stands for allowed) and string to explain
        the reason for denying execution.
    """
    if not permissions:
        return (True, "")

    for resource in resources if isinstance(resources, list) else [resources]:
        logger.debug(
            f"Enforcing permission policies for {type(resource)}:{resource.name} to execute {actions}"
        )
        matching_permissions = [
            p
            for p in permissions
            if p.match_resource(resources) and p.match_actions(actions)
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
                    return grant, ",".join(explanations)
    else:
        message = f"No permissions defined to manage {actions} on {type(resources)}:{resources.name}."
        logger.info(f"**PERMISSION GRANTED**: {message}")
    return (True, "")
