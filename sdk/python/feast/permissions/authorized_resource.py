import logging
import re
from typing import Any, Optional, Union, get_args

from feast.feast_object import FeastObject

"""
Constant to refer to all the managed types.
"""
ALL_RESOURCE_TYPES = list(get_args(FeastObject))

logger = logging.getLogger(__name__)


class AuthzedResource:
    """
    Identifies the protected resources by class type and optional filters
    based on the resource name and tags.

    Attributes:
        type: the list of secured resource types (must match one Feast class, as defined by the FeastObject type)
        name_pattern: a regex to match the resource name. Defaults to None, meaning that no name filtering is applied
        required_tags: dictionary of key-value pairs that must match the resource tags. All these required_tags must be present as resource
        tags with the given value. Defaults to None, meaning that no tags filtering is applied.
    """

    type: list[FeastObject]
    with_subclasses: bool
    name_pattern: Optional[str]
    required_tags: Optional[dict[str, str]]

    def __init__(
        self,
        type: Union[list[FeastObject], FeastObject],
        with_subclasses: bool = False,
        name_pattern: Optional[str] = None,
        required_tags: Optional[dict[str, str]] = None,
    ):
        _type: list[FeastObject] = type if isinstance(type, list) else [type]
        for t in _type:
            if t not in get_args(FeastObject):
                raise ValueError(f"{t} is not one of the managed types")

        self.type = _type
        self.with_subclasses = with_subclasses
        self.name_pattern = _normalize_name_pattern(name_pattern)
        self.required_tags = _normalize_required_tags(required_tags)

    def __str__(self):
        return f"AuthzedResource(type={self.type}, with_subclasses={self.with_subclasses}, name_pattern={self.name_pattern}, required_tags={self.required_tags})"

    def __eq__(self, other):
        if not isinstance(other, AuthzedResource):
            return NotImplemented
        return (
            self.type == other.type
            and self.with_subclasses == other.with_subclasses
            and self.name_pattern == other.name_pattern
            and self.required_tags == other.required_tags
        )

    def match_resource(self, resource: FeastObject) -> bool:
        if resource is None:
            logger.warning(f"None passed to {self.match_resource.__name__}")
            return False

        if not is_of_expected_type(resource):
            logger.warning(
                f"Given resource is not of a managed type but {type(resource)}"
            )
            return False

        if not any(isinstance(resource, t) for t in self.type):
            logger.info(f"Resource does not match any of the expected type {self.type}")
            return False

        if self.name_pattern is not None:
            if hasattr(resource, "name"):
                if isinstance(resource.name, str):
                    match = bool(re.fullmatch(self.name_pattern, resource.name))
                    if not match:
                        logger.info(
                            f"Resource name {resource.name} does not match pattern {self.name_pattern}"
                        )
                        return False
                else:
                    logger.warning(
                        f"Resource {resource} has no `name` attribute of unexpected type {type(resource.name)}"
                    )
            else:
                logger.warning(f"Resource {resource} has no `name` attribute")

        if self.required_tags:
            if hasattr(resource, "tags"):
                if isinstance(resource.tags, dict):
                    for tag in self.required_tags.keys():
                        required_value = self.required_tags.get(tag)
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


def is_of_expected_type(resource: Any):
    for t in get_args(FeastObject):
        # Use isinstance to pass Mock validation
        if isinstance(resource, t):
            return True
    return False


def _normalize_name_pattern(name_pattern: Optional[str]):
    if name_pattern is not None:
        return name_pattern.strip()
    return None


def _normalize_required_tags(required_tags: Optional[dict[str, str]]):
    if required_tags:
        return {
            k.strip(): v.strip() if isinstance(v, str) else v
            for k, v in required_tags.items()
        }
    return None
