import logging

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
    resource: FeastObject,
    actions: list[AuthzedAction],
) -> tuple[bool, str]:
    """
    Defines the logic to apply the configured permissions when a given action is requested on
    a protected resource.

    Args:
        role_manager: The `RoleManager` instance.
        permissions: The configured set of `Permission`.
        user: The current user.
        resource: The resource for which we need to enforce authorized permission.
        actions: The requested actions to be authorized.
    """

    if not permissions:
        return (True, "")

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
                f"Permission ({p.name})",
                permission_grant,
                f"Permission {p.name} denied access: {permission_explanation}",
            )

            if evaluator.is_decided():
                grant, explanations = evaluator.grant()
                return grant, ",".join(explanations)
    else:
        message = f"No permissions defined to manage {actions} on {type(resource)}:{resource.name}."
        logger.info(f"**PERMISSION GRANTED**: {message}")
    return (True, "")
