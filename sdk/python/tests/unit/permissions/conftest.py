from unittest.mock import Mock

import pytest

from feast import FeatureView
from feast.infra.registry.base_registry import BaseRegistry
from feast.permissions.decision import DecisionStrategy
from feast.permissions.decorator import require_permissions
from feast.permissions.permission import AuthzedAction, Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.permissions.security_manager import (
    SecurityManager,
    set_security_manager,
)
from feast.permissions.user import User
from feast.project_metadata import ProjectMetadata


class SecuredFeatureView(FeatureView):
    def __init__(self, name, tags):
        super().__init__(
            name=name,
            source=Mock(),
            tags=tags,
        )

    @require_permissions(actions=[AuthzedAction.READ])
    def read_protected(self) -> bool:
        return True

    @require_permissions(actions=[AuthzedAction.UPDATE])
    def write_protected(self) -> bool:
        return True

    def unprotected(self) -> bool:
        return True


@pytest.fixture
def feature_views() -> list[FeatureView]:
    return [
        SecuredFeatureView("secured", {}),
        SecuredFeatureView("special-secured", {}),
    ]


@pytest.fixture
def users() -> list[User]:
    users = []
    users.append(User("r", ["reader"]))
    users.append(User("w", ["writer"]))
    users.append(User("rw", ["reader", "writer"]))
    users.append(User("admin", ["reader", "writer", "admin"]))
    return dict([(u.username, u) for u in users])


@pytest.fixture
def security_manager() -> SecurityManager:
    permissions = []
    permissions.append(
        Permission(
            name="reader",
            types=FeatureView,
            with_subclasses=True,
            policy=RoleBasedPolicy(roles=["reader"]),
            actions=[AuthzedAction.READ],
        )
    )
    permissions.append(
        Permission(
            name="writer",
            types=FeatureView,
            with_subclasses=True,
            policy=RoleBasedPolicy(roles=["writer"]),
            actions=[AuthzedAction.UPDATE],
        )
    )
    permissions.append(
        Permission(
            name="special",
            types=FeatureView,
            with_subclasses=True,
            name_pattern="special.*",
            policy=RoleBasedPolicy(roles=["admin", "special-reader"]),
            actions=[AuthzedAction.READ, AuthzedAction.UPDATE],
        )
    )

    project_metadata = ProjectMetadata(project_name="test_project")
    registry = Mock(spec=BaseRegistry)
    registry.list_permissions = Mock(return_value=permissions)
    registry.list_project_metadata = Mock(return_value=[project_metadata])
    registry.get_decision_strategy = Mock(return_value=DecisionStrategy.UNANIMOUS)
    sm = SecurityManager(project="any", registry=registry)
    set_security_manager(sm)
    return sm
