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


@pytest.mark.parametrize(
    "username, requested_actions, allowed, allowed_single, raise_error_in_assert, raise_error_in_permit",
    [
        (None, [], False, [False, False], [True, True], False),
        ("r", [AuthzedAction.DESCRIBE], True, [True, True], [False, False], False),
        ("r", [AuthzedAction.UPDATE], False, [False, False], [True, True], False),
        ("w", [AuthzedAction.DESCRIBE], False, [False, False], [True, True], False),
        ("w", [AuthzedAction.UPDATE], False, [True, True], [False, False], False),
        ("rw", [AuthzedAction.DESCRIBE], False, [True, True], [False, False], False),
        ("rw", [AuthzedAction.UPDATE], False, [True, True], [False, False], False),
        (
            "rw",
            [AuthzedAction.DESCRIBE, AuthzedAction.UPDATE],
            False,
            [False, False],
            [True, True],
            True,
        ),
        (
            "special",
            [AuthzedAction.DESCRIBE, AuthzedAction.UPDATE],
            False,
            [False, True],
            [True, False],
            True,
        ),
        (
            "special",
            READ + [AuthzedAction.UPDATE],
            False,
            [False, False],
            [True, True],
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
):
    sm = security_manager
    resources = feature_views

    user = users.get(username)
    sm.set_current_user(user)

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
    "username, allowed",
    [
        (None, False),
        ("r", False),
        ("w", False),
        ("rw", False),
        ("special", False),
        ("updater", False),
        ("creator", True),
        ("admin", True),
    ],
)
def test_create_entity(
    security_manager,
    users,
    username,
    allowed,
):
    sm = security_manager
    entity = Entity(
        name="",
    )

    user = users.get(username)
    sm.set_current_user(user)

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
    "username, allowed",
    [
        (None, False),
        ("r", False),
        ("w", False),
        ("rw", False),
        ("special", False),
        ("updater", True),
        ("creator", False),
        ("admin", True),
    ],
)
def test_update_entity(
    security_manager,
    users,
    username,
    allowed,
):
    sm = security_manager
    entity = Entity(
        name="",
    )

    user = users.get(username)
    sm.set_current_user(user)

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
