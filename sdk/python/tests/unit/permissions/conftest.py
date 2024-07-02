from unittest.mock import Mock

import pytest

from feast import FeatureView
from feast.permissions.decorator import require_permissions
from feast.permissions.permission import AuthzedAction, Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.permissions.role_manager import RoleManager
from feast.permissions.security_manager import (
    SecurityManager,
    set_security_manager,
)


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
def role_manager() -> RoleManager:
    rm = RoleManager()
    rm.add_roles_for_user("r", ["reader"])
    rm.add_roles_for_user("w", ["writer"])
    rm.add_roles_for_user("rw", ["reader", "writer"])
    return rm


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

    rm = RoleManager()
    rm.add_roles_for_user("r", ["reader"])
    rm.add_roles_for_user("w", ["writer"])
    rm.add_roles_for_user("rw", ["reader", "writer"])
    rm.add_roles_for_user("admin", ["reader", "writer", "admin"])
    sm = SecurityManager(role_manager=rm, permissions=permissions)
    set_security_manager(sm)
    return sm
