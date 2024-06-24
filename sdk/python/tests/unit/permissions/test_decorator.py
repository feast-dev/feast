from unittest.mock import Mock

import assertpy
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

    @require_permissions(actions=[AuthzedAction.WRITE])
    def write_protected(self) -> bool:
        return True

    def unprotected(self) -> bool:
        return True


@pytest.fixture
def security_manager() -> SecurityManager:
    permissions = []
    permissions.append(
        Permission(
            name="reader",
            types=FeatureView,
            with_subclasses=True,
            policies=[RoleBasedPolicy(roles=["reader"])],
            actions=[AuthzedAction.READ],
        )
    )
    permissions.append(
        Permission(
            name="writer",
            types=FeatureView,
            with_subclasses=True,
            policies=[RoleBasedPolicy(roles=["writer"])],
            actions=[AuthzedAction.WRITE],
        )
    )

    rm = RoleManager()
    rm.add_roles_for_user("r", ["reader"])
    rm.add_roles_for_user("w", ["writer"])
    rm.add_roles_for_user("rw", ["reader", "writer"])
    sm = SecurityManager(role_manager=rm, permissions=permissions)
    set_security_manager(sm)
    return sm


@pytest.fixture
def feature_view() -> FeatureView:
    return SecuredFeatureView("secured", {})


@pytest.mark.parametrize(
    "user, can_read, can_write",
    [
        (None, False, False),
        ("r", True, False),
        ("w", False, True),
        ("rw", True, True),
    ],
)
def test_access_SecuredFeatureView(
    security_manager, feature_view, user, can_read, can_write
):
    sm = security_manager
    fv = feature_view

    sm.set_current_user(user)
    if can_read:
        fv.read_protected()
    else:
        with pytest.raises(PermissionError):
            fv.read_protected()
    if can_write:
        fv.write_protected()
    else:
        with pytest.raises(PermissionError):
            fv.write_protected()
    assertpy.assert_that(fv.unprotected()).is_true()
