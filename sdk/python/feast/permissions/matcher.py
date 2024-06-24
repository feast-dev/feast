import logging
import re
from typing import Any, Optional, Union, get_args
from unittest.mock import Mock

from feast.feast_object import FeastObject
from feast.permissions.action import AuthzedAction

logger = logging.getLogger(__name__)


def is_a_feast_object(resource: Any):
    for t in get_args(FeastObject):
        # Use isinstance to pass Mock validation
        if isinstance(resource, t):
            return True
    return False


def resource_match_config(
    resource: FeastObject,
    expected_types: list[FeastObject],
    with_subclasses: bool = True,
    name_pattern: Optional[str] = None,
    required_tags: Optional[dict[str, str]] = None,
) -> bool:
    if resource is None:
        logger.warning(f"None passed to {resource_match_config.__name__}")
        return False

    if not is_a_feast_object(resource):
        logger.warning(f"Given resource is not of a managed type but {type(resource)}")
        return False

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
    actions: list[AuthzedAction],
    allowed_actions: Union[list[AuthzedAction], AuthzedAction] = AuthzedAction.ALL,
) -> bool:
    if AuthzedAction.ALL in allowed_actions:
        return True

    return all(a in allowed_actions for a in actions)
