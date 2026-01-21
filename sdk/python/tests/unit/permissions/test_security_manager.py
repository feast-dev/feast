import assertpy
import pytest

from feast.entity import Entity
from feast.errors import FeastObjectNotFoundException, FeastPermissionError
from feast.permissions.action import READ, AuthzedAction
from feast.permissions.security_manager import (
    assert_permissions,
    assert_permissions_to_update,
    permitted_resources,
)
from feast.permissions.user import User


@pytest.mark.parametrize(
    "username, requested_actions, allowed, allowed_single, raise_error_in_assert, raise_error_in_permit, intra_communication_flag",
    [
        (None, [], False, [False, False], [True, True], True, False),
        (None, [], True, [True, True], [False, False], False, True),
        (
            "r",
            [AuthzedAction.DESCRIBE],
            True,
            [True, True],
            [False, False],
            False,
            False,
        ),
        (
            "r",
            [AuthzedAction.DESCRIBE],
            True,
            [True, True],
            [False, False],
            False,
            True,
        ),
        ("server_intra_com_val", [], True, [True, True], [False, False], False, True),
        (
            "r",
            [AuthzedAction.UPDATE],
            False,
            [False, False],
            [True, True],
            True,
            False,
        ),
        ("r", [AuthzedAction.UPDATE], True, [True, True], [False, False], False, True),
        (
            "w",
            [AuthzedAction.DESCRIBE],
            False,
            [False, False],
            [True, True],
            True,
            False,
        ),
        (
            "w",
            [AuthzedAction.DESCRIBE],
            True,
            [True, True],
            [True, True],
            False,
            True,
        ),
        (
            "w",
            [AuthzedAction.UPDATE],
            False,
            [True, True],
            [False, False],
            False,
            False,
        ),
        ("w", [AuthzedAction.UPDATE], False, [True, True], [False, False], False, True),
        (
            "rw",
            [AuthzedAction.DESCRIBE],
            False,
            [True, True],
            [False, False],
            False,
            False,
        ),
        (
            "rw",
            [AuthzedAction.DESCRIBE],
            False,
            [True, True],
            [False, False],
            False,
            True,
        ),
        (
            "rw",
            [AuthzedAction.UPDATE],
            False,
            [True, True],
            [False, False],
            False,
            False,
        ),
        (
            "rw",
            [AuthzedAction.UPDATE],
            False,
            [True, True],
            [False, False],
            False,
            True,
        ),
        (
            "rw",
            [AuthzedAction.DESCRIBE, AuthzedAction.UPDATE],
            False,
            [False, False],
            [True, True],
            False,
            False,
        ),
        (
            "rw",
            [AuthzedAction.DESCRIBE, AuthzedAction.UPDATE],
            True,
            [True, True],
            [False, False],
            False,
            True,
        ),
        (
            "special",
            [AuthzedAction.DESCRIBE, AuthzedAction.UPDATE],
            False,
            [False, True],
            [True, False],
            False,
            False,
        ),
        (
            "admin",
            [AuthzedAction.DESCRIBE, AuthzedAction.UPDATE],
            True,
            [True, True],
            [False, False],
            False,
            True,
        ),
        (
            "special",
            READ + [AuthzedAction.UPDATE],
            False,
            [False, False],
            [True, True],
            False,
            False,
        ),
        (
            "admin",
            READ + [AuthzedAction.UPDATE],
            True,
            [True, True],
            [False, False],
            False,
            True,
        ),
    ],
)
def test_access_SecuredFeatureView(
    security_manager,
    feature_views,
    users,
    username,
    requested_actions,
    allowed,
    allowed_single,
    raise_error_in_assert,
    raise_error_in_permit,
    intra_communication_flag,
    monkeypatch,
):
    sm = security_manager
    user = users.get(username)
    sm.set_current_user(user)

    if intra_communication_flag:
        monkeypatch.setenv("INTRA_COMMUNICATION_BASE64", "server_intra_com_val")
        sm.set_current_user(User("server_intra_com_val", []))
    else:
        monkeypatch.delenv("INTRA_COMMUNICATION_BASE64", False)

    resources = feature_views

    result = []
    if raise_error_in_permit:
        with pytest.raises(FeastPermissionError):
            result = permitted_resources(resources=resources, actions=requested_actions)
    else:
        result = permitted_resources(resources=resources, actions=requested_actions)

    if allowed:
        assertpy.assert_that(result).is_equal_to(resources)
    elif not raise_error_in_permit:
        filtered = [r for i, r in enumerate(resources) if allowed_single[i]]
        assertpy.assert_that(result).is_equal_to(filtered)

    for i, r in enumerate(resources):
        if allowed_single[i]:
            result = assert_permissions(resource=r, actions=requested_actions)
            assertpy.assert_that(result).is_equal_to(r)
        elif raise_error_in_assert[i]:
            with pytest.raises(FeastPermissionError):
                assert_permissions(resource=r, actions=requested_actions)
        else:
            result = assert_permissions(resource=r, actions=requested_actions)
            assertpy.assert_that(result).is_none()


@pytest.mark.parametrize(
    "username, allowed, intra_communication_flag",
    [
        (None, False, False),
        (None, True, True),
        ("r", False, False),
        ("r", True, True),
        ("w", False, False),
        ("w", True, True),
        ("rw", False, False),
        ("rw", True, True),
        ("special", False, False),
        ("special", True, True),
        ("updater", False, False),
        ("updater", True, True),
        ("creator", True, False),
        ("creator", True, True),
        ("admin", True, False),
        ("admin", True, True),
    ],
)
def test_create_entity(
    security_manager,
    users,
    username,
    allowed,
    intra_communication_flag,
    monkeypatch,
):
    sm = security_manager
    user = users.get(username)
    sm.set_current_user(user)

    if intra_communication_flag:
        monkeypatch.setenv("INTRA_COMMUNICATION_BASE64", "server_intra_com_val")
        sm.set_current_user(User("server_intra_com_val", []))
    else:
        monkeypatch.delenv("INTRA_COMMUNICATION_BASE64", False)

    entity = Entity(
        name="",
    )

    def getter(name: str, project: str, allow_cache: bool):
        raise FeastObjectNotFoundException()

    if allowed:
        result = assert_permissions_to_update(
            resource=entity, getter=getter, project=""
        )
        assertpy.assert_that(result).is_equal_to(entity)
    else:
        with pytest.raises(FeastPermissionError):
            assert_permissions_to_update(resource=entity, getter=getter, project="")


@pytest.mark.parametrize(
    "username, allowed, intra_communication_flag",
    [
        (None, False, False),
        (None, True, True),
        ("r", False, False),
        ("r", True, True),
        ("w", False, False),
        ("w", True, True),
        ("rw", False, False),
        ("rw", True, True),
        ("special", False, False),
        ("special", True, True),
        ("updater", True, False),
        ("updater", True, True),
        ("creator", False, False),
        ("creator", True, True),
        ("admin", True, False),
        ("admin", True, True),
    ],
)
def test_update_entity(
    security_manager,
    users,
    username,
    allowed,
    intra_communication_flag,
    monkeypatch,
):
    sm = security_manager
    user = users.get(username)
    sm.set_current_user(user)

    if intra_communication_flag:
        monkeypatch.setenv("INTRA_COMMUNICATION_BASE64", "server_intra_com_val")
        sm.set_current_user(User("server_intra_com_val", []))
    else:
        monkeypatch.delenv("INTRA_COMMUNICATION_BASE64", False)

    entity = Entity(
        name="",
    )

    def getter(name: str, project: str, allow_cache: bool):
        return entity

    if allowed:
        result = assert_permissions_to_update(
            resource=entity, getter=getter, project=""
        )
        assertpy.assert_that(result).is_equal_to(entity)
    else:
        with pytest.raises(FeastPermissionError):
            assert_permissions_to_update(resource=entity, getter=getter, project="")
