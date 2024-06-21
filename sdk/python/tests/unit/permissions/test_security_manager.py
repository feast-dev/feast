from unittest.mock import Mock

import assertpy
import pytest

from feast import FeatureView
from feast.permissions.permission import AuthzedAction, Permission
from feast.permissions.policy import RoleBasedPolicy
from feast.permissions.role_manager import RoleManager
from feast.permissions.security_manager import (
    SecurityManager,
    require_permissions,
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


def test_no_user(security_manager, feature_view):
    fv = feature_view

    with pytest.raises(PermissionError):
        fv.read_protected()
    assertpy.assert_that(fv.unprotected()).is_true()


def test_reader_user(security_manager, feature_view):
    sm = security_manager
    fv = feature_view
    sm.set_current_user("r")
    fv.read_protected()
    assertpy.assert_that(fv.unprotected()).is_true()


def test_writer_user(security_manager, feature_view):
    sm = security_manager
    fv = feature_view
    sm.set_current_user("w")
    with pytest.raises(PermissionError):
        fv.read_protected()
    assertpy.assert_that(fv.unprotected()).is_true()


def test_reader_writer_user(security_manager, feature_view):
    sm = security_manager
    fv = feature_view
    sm.set_current_user("rw")
    fv.read_protected()
    assertpy.assert_that(fv.unprotected()).is_true()
