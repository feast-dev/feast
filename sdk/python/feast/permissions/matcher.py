"""
This module provides utility matching functions.
"""

import logging
import re
from typing import Any, Optional, get_args
from unittest.mock import Mock

from feast.feast_object import FeastObject
from feast.permissions.action import AuthzedAction

logger = logging.getLogger(__name__)


def is_a_feast_object(resource: Any):
    """
    A matcher to verify that a given object is one of the Feast objects defined in the `FeastObject` type.

    Args:
        resource: An object instance to verify.
    Returns:
        `True` if the given object is one of the types in the FeastObject alias or a subclass of one of them.
    """
    for t in get_args(FeastObject):
        # Use isinstance to pass Mock validation
        if isinstance(resource, t):
            return True
    return False


def _get_type(resource: FeastObject) -> Any:
    is_mock = isinstance(resource, Mock)
    if not is_mock:
        return type(resource)
    else:
        return getattr(resource, "_spec_class", None)


def _is_abstract_type(type: Any) -> bool:
    return bool(getattr(type, "__abstractmethods__", False))


def resource_match_config(
    resource: FeastObject,
    expected_types: list[FeastObject],
    with_subclasses: bool = True,
    name_pattern: Optional[str] = None,
    required_tags: Optional[dict[str, str]] = None,
) -> bool:
    """
    Match a given Feast object against the configured type, name and tags in a permission configuration.

    Args:
        resource: A FeastObject instance to match agains the permission.
        expected_types: The list of object types configured in the permission.
        with_subclasses: `True` if the type match includes sub-classes, `False` if the type match is exact.
        name_pattern: The optional name pattern filter configured in the permission.
        required_tags: The optional dicstionary of required tags configured in the permission.

    Returns:
        bool: `True` if the resource matches the configured permission filters.
    """
    if resource is None:
        logger.warning(f"None passed to {resource_match_config.__name__}")
        return False

    _type = _get_type(resource)
    if not is_a_feast_object(resource):
        logger.warning(f"Given resource is not of a managed type but {_type}")
        return False

    is_abstract = _is_abstract_type(_type)
    if is_abstract and not with_subclasses:
        logger.debug(
            f"Overriding default configuration for abstract type {_type}: with_subclasses=True"
        )
        with_subclasses = True

    if with_subclasses:
        # mypy check ignored because of https://github.com/python/mypy/issues/11673, or it raises "Argument 2 to "isinstance" has incompatible type "tuple[Featu ..."
        if not isinstance(resource, tuple(expected_types)):  # type: ignore
            logger.info(
                f"Resource does not match any of the expected type {expected_types} (with_subclasses={with_subclasses})"
            )
            return False
    else:
        is_mock = isinstance(resource, Mock)
        exact_type_match = False
        for t in expected_types:
            if not is_mock:
                if type(resource) is t:
                    exact_type_match = True
                    break
            else:
                if getattr(resource, "_spec_class", None) is t:
                    exact_type_match = True
                    break
        if not exact_type_match:
            logger.info(
                f"Resource does not match any of the expected type {expected_types} (with_subclasses={with_subclasses})"
            )
            return False

    if name_pattern is not None:
        if hasattr(resource, "name"):
            if isinstance(resource.name, str):
                match = bool(re.fullmatch(name_pattern, resource.name))
                if not match:
                    logger.info(
                        f"Resource name {resource.name} does not match pattern {name_pattern}"
                    )
                    return False
            else:
                logger.warning(
                    f"Resource {resource} has no `name` attribute of unexpected type {type(resource.name)}"
                )
        else:
            logger.warning(f"Resource {resource} has no `name` attribute")

    if required_tags:
        if hasattr(resource, "tags"):
            if isinstance(resource.tags, dict):
                for tag in required_tags.keys():
                    required_value = required_tags.get(tag)
                    actual_value = resource.tags.get(tag)
                    if required_value != actual_value:
                        logger.info(
                            f"Unmatched value {actual_value} for tag {tag}: expected {required_value}"
                        )
                        return False
            else:
                logger.warning(
                    f"Resource {resource} has no `tags` attribute of unexpected type {type(resource.tags)}"
                )
        else:
            logger.warning(f"Resource {resource} has no `tags` attribute")

    return True


def actions_match_config(
    requested_actions: list[AuthzedAction],
    allowed_actions: list[AuthzedAction],
) -> bool:
    """
    Match a list of actions against the actions defined in a permission configuration.

    Args:
        requested_actions: A list of actions to be executed.
        allowed_actions: The list of actions configured in the permission.

    Returns:
        bool: `True` if all the given `requested_actions` are defined in the `allowed_actions`.
        Whatever the `requested_actions`, it returns `True` if `allowed_actions` includes `AuthzedAction.ALL`
    """
    if AuthzedAction.ALL in allowed_actions:
        return True

    return all(a in allowed_actions for a in requested_actions)
