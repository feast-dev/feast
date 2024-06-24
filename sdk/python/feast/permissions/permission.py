import logging
from abc import ABC
from typing import Optional, Union, get_args

from feast.feast_object import FeastObject
from feast.permissions.action import AuthzedAction
from feast.permissions.decision import DecisionStrategy
from feast.permissions.matcher import actions_match_config, resource_match_config
from feast.permissions.policy import AllowAll, Policy

logger = logging.getLogger(__name__)


"""
Constant to refer to all the managed types.
"""
ALL_RESOURCE_TYPES = list(get_args(FeastObject))


class Permission(ABC):
    """
    The Permission class defines the authorization policies to be validated whenever the identified actions are
    requested on the matching resources.

    Attributes:
        name: The permission name (can be duplicated, used for logging troubleshooting)
        types: The list of protected resource  types as defined by the `FeastObject` type. Defaults to all managed types (e.g. the `ALL_RESOURCE_TYPES` constant)
        with_subclasses: If `True`, it includes subclasses of the given types in the match, otherwise only precise type match is applied. Defaults to `True`.
        name_pattern: a regex to match the resource name. Defaults to None, meaning that no name filtering is applied
        required_tags: dictionary of key-value pairs that must match the resource tags. All these required_tags must be present as resource
        tags with the given value. Defaults to None, meaning that no tags filtering is applied.
        actions: The actions authorized by this permission. Defaults to `AuthzedAction.ALL`.
        policies: The policies to be applied to validate a client request.
        decision_strategy: The strategy to apply in case of multiple policies.
    """

    _name: str
    _types: list[FeastObject]
    _with_subclasses: bool
    _name_pattern: Optional[str]
    _required_tags: Optional[dict[str, str]]
    _actions: list[AuthzedAction]
    _policies: list[Policy]
    _decision_strategy: DecisionStrategy

    def __init__(
        self,
        name: str,
        types: Union[list[FeastObject], FeastObject] = ALL_RESOURCE_TYPES,
        with_subclasses: bool = True,
        name_pattern: Optional[str] = None,
        required_tags: Optional[dict[str, str]] = None,
        actions: Union[list[AuthzedAction], AuthzedAction] = AuthzedAction.ALL,
        policies: Union[list[Policy], Policy] = AllowAll,
        decision_strategy: DecisionStrategy = DecisionStrategy.UNANIMOUS,
    ):
        if not types:
            raise ValueError("The list 'types' must be non-empty.")
        for t in types if isinstance(types, list) else [types]:
            if t not in get_args(FeastObject):
                raise ValueError(f"{t} is not one of the managed types")
        if actions is None or not actions:
            raise ValueError("The list 'actions' must be non-empty.")
        if not policies:
            raise ValueError("The list 'policies' must be non-empty.")
        if decision_strategy not in DecisionStrategy.__members__.values():
            raise ValueError(
                "The 'decision_strategy' must be one of the allowed values."
            )
        self._name = name
        self._types = types if isinstance(types, list) else [types]
        self._with_subclasses = with_subclasses
        self._name_pattern = _normalize_name_pattern(name_pattern)
        self._required_tags = _normalize_required_tags(required_tags)
        self._actions = actions if isinstance(actions, list) else [actions]
        self._policies = policies if isinstance(policies, list) else [policies]
        self._decision_strategy = decision_strategy

    _global_decision_strategy: DecisionStrategy = DecisionStrategy.UNANIMOUS

    @staticmethod
    def get_global_decision_strategy() -> DecisionStrategy:
        return Permission._global_decision_strategy

    @staticmethod
    def set_global_decision_strategy(global_decision_strategy: DecisionStrategy):
        """
        Defines the global decision strategy to be applied if multiple permissions match the same resource.
        """
        Permission._global_decision_strategy = global_decision_strategy

    @property
    def name(self) -> str:
        return self._name

    @property
    def types(self) -> list[FeastObject]:
        return self._types

    @property
    def with_subclasses(self) -> bool:
        return self._with_subclasses

    @property
    def name_pattern(self) -> Optional[str]:
        return self._name_pattern

    @property
    def required_tags(self) -> Optional[dict[str, str]]:
        return self._required_tags

    @property
    def actions(self) -> list[AuthzedAction]:
        return self._actions

    @property
    def policies(self) -> list[Policy]:
        return self._policies

    @property
    def decision_strategy(self) -> DecisionStrategy:
        return self._decision_strategy

    def match_resource(self, resource: FeastObject) -> bool:
        return resource_match_config(
            resource=resource,
            expected_types=self.types,
            with_subclasses=self.with_subclasses,
            name_pattern=self.name_pattern,
            required_tags=self.required_tags,
        )

    def match_actions(self, actions: list[AuthzedAction]) -> bool:
        return actions_match_config(
            allowed_actions=self.actions,
            actions=actions,
        )


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
