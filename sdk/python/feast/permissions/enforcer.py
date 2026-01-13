import logging

from feast.errors import FeastPermissionError
from feast.feast_object import FeastObject
from feast.permissions.decision import DecisionEvaluator
from feast.permissions.permission import (
    AuthzedAction,
    Permission,
)
from feast.permissions.user import User

logger = logging.getLogger(__name__)


def enforce_policy(
    permissions: list[Permission],
    user: User,
    resources: list[FeastObject],
    actions: list[AuthzedAction],
    filter_only: bool = False,
) -> list[FeastObject]:
    """
    Define the logic to apply the configured permissions when a given action is requested on
    a protected resource.

    If no permissions are defined, the result is to deny the execution.

    Args:
        permissions: The configured set of `Permission`.
        user: The current user.
        resources: The resources for which we need to enforce authorized permission.
        actions: The requested actions to be authorized.
        filter_only: If `True`, it removes unauthorized resources from the returned value, otherwise it raises a `FeastPermissionError` the
        first unauthorized resource. Defaults to `False`.

    Returns:
        list[FeastObject]: A filtered list of the permitted resources.

    Raises:
        FeastPermissionError: If the current user is not authorized to eecute the requested actions on the given resources (and `filter_only` is `False`).
    """
    if not permissions:
        # If no permissions are defined, deny access to all resources
        # This is a security measure to prevent unauthorized access
        logger.warning("No permissions defined - denying access to all resources")
        raise FeastPermissionError(
            "Permissions are not defined - access denied for all resources"
        )

    _permitted_resources: list[FeastObject] = []
    for resource in resources:
        logger.debug(
            f"Enforcing permission policies for {type(resource).__name__}:{resource.name} to execute {actions}"
        )
        matching_permissions = [
            p
            for p in permissions
            if p.match_resource(resource) and p.match_actions(actions)
        ]

        if matching_permissions:
            evaluator = DecisionEvaluator(len(matching_permissions))
            for p in matching_permissions:
                permission_grant, permission_explanation = p.policy.validate_user(
                    user=user
                )
                evaluator.add_grant(
                    permission_grant,
                    f"Permission {p.name} denied execution of {[a.value.upper() for a in actions]} to {type(resource).__name__}:{resource.name}: {permission_explanation}",
                )

                if evaluator.is_decided():
                    grant, explanations = evaluator.grant()
                    if not grant:
                        if filter_only and p.name_patterns:
                            continue
                        logger.error(f"Permission denied: {','.join(explanations)}")
                        raise FeastPermissionError(",".join(explanations))
                    logger.debug(
                        f"Permission granted for {type(resource).__name__}:{resource.name}"
                    )
                    _permitted_resources.append(resource)
                    break
        else:
            if not filter_only:
                message = f"No permissions defined to manage {actions} on {type(resource)}/{resource.name}."
                logger.exception(f"**PERMISSION NOT GRANTED**: {message}")
                raise FeastPermissionError(message)
            else:
                # filter_only=True: Check if there are permissions for this resource type
                resource_type_permissions = [
                    p
                    for p in permissions
                    if any(isinstance(resource, t) for t in p.types)  # type: ignore
                ]
                if not resource_type_permissions:
                    # No permissions exist for this resource type - should raise error
                    message = f"No permissions defined to manage {actions} on {type(resource)}/{resource.name}."
                    logger.exception(f"**PERMISSION NOT GRANTED**: {message}")
                    raise FeastPermissionError(message)
                elif not any(p.name_patterns for p in resource_type_permissions):
                    # Permissions exist for this resource type but no name_patterns - should raise error
                    message = f"No permissions defined to manage {actions} on {type(resource)}/{resource.name}."
                    logger.exception(f"**PERMISSION NOT GRANTED**: {message}")
                    raise FeastPermissionError(message)
                else:
                    # Permissions exist for this resource type with name_patterns - filter out this resource
                    logger.debug(
                        f"Filtering out {type(resource).__name__}:{resource.name} - no matching permissions"
                    )
                    continue
    return _permitted_resources
