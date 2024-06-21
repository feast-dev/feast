import enum
from abc import ABC
from typing import Optional, Union

from feast.feast_object import FeastObject
from feast.permissions.authorized_resource import ALL_RESOURCE_TYPES, AuthzedResource
from feast.permissions.decision import DecisionStrategy
from feast.permissions.policy import AllowAll, Policy


class AuthzedAction(enum.Enum):
    """
    Identifies the type of action being secured by the permissions framework, according to the familiar CRUD and Feast terminology.
    """

    ALL = "all"  # All actions
    CREATE = "create"  # Create an instance
    READ = "read"  # Access the instance state
    UPDATE = "update"  # Update the instance state
    DELETE = "delete"  # Deelete an instance
    QUERY = "query"  # Query both online and online stores
    QUERY_ONLINE = "query_online"  # Query the online store only
    QUERY_OFFLINE = "query_online"  # Query the offline store only
    WRITE = "write"  # Query on any store
    WRITE_ONLINE = "write_online"  # Write to the online store only
    WRITE_OFFLINE = "write_offline"  # Write to the offline store only


class Permission(ABC):
    """
    The Permission class defines the authorization policies to be validated whenever the identified actions are
    requested on the matching resources.

    Attributes:
        name: The permission name (can be duplicated, used for logging troubleshooting)
        resources: The list of protected resource (as `AuthzedResource` type). Defaults to `ALL`.
        actions: The actions authorized by this permission. Defaults to `AuthzedAction.ALL`.
        policies: The policies to be applied to validate a client request.
        decision_strategy: The strategy to apply in case of multiple policies.
    """

    _name: str
    _resources: list[AuthzedResource]
    _actions: list[AuthzedAction]
    _policies: list[Policy]
    _decision_strategy: DecisionStrategy

    def __init__(
        self,
        name: str,
        resources: Optional[
            Union[str, list[AuthzedResource], AuthzedResource]
        ] = ALL_RESOURCE_TYPES,
        actions: Optional[
            Union[list[AuthzedAction], AuthzedAction]
        ] = AuthzedAction.ALL,
        policies: Optional[Union[list[Policy], Policy]] = AllowAll,
        decision_strategy: Optional[DecisionStrategy] = DecisionStrategy.UNANIMOUS,
    ):
        if not resources:
            raise ValueError("The list 'resources' must be non-empty.")
        if actions is None or not actions:
            raise ValueError("The list 'actions' must be non-empty.")
        if not policies:
            raise ValueError("The list 'policies' must be non-empty.")
        if decision_strategy not in DecisionStrategy.__members__.values():
            raise ValueError(
                "The 'decision_strategy' must be one of the allowed values."
            )
        self._name = name
        self._resources = resources if isinstance(resources, list) else [resources]
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
    def resources(self) -> list[AuthzedResource]:
        return self._resources

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
        matches = [
            authzed_resource
            for authzed_resource in self._resources
            if authzed_resource.match_resource(resource)
        ]
        return len(matches) > 0

    def match_actions(self, actions: list[AuthzedAction]) -> bool:
        if AuthzedAction.ALL in self._actions:
            return True

        return all(a in self._actions for a in actions)
