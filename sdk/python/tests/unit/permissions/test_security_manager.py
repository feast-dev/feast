import assertpy
import pytest

from feast.permissions.action import READ, AuthzedAction
from feast.permissions.security_manager import assert_permissions, permitted_resources


@pytest.mark.parametrize(
    "username, requested_actions, allowed, allowed_single, raise_error_in_assert, raise_error_in_permit, intra_communication_flag",
    [
        (None, [], False, [False, False], [True, True], False, False),
        (None, [], False, [False, False], [True, True], False, True),
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
        ("test1234", [], True, [True, True], [False, False], False, True),
        (
            "r",
            [AuthzedAction.UPDATE],
            False,
            [False, False],
            [True, True],
            False,
            False,
        ),
        ("r", [AuthzedAction.UPDATE], False, [False, False], [True, True], False, True),
        (
            "w",
            [AuthzedAction.DESCRIBE],
            False,
            [False, False],
            [True, True],
            False,
            False,
        ),
        (
            "w",
            [AuthzedAction.DESCRIBE],
            False,
            [False, False],
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
            True,
            False,
        ),
        (
            "rw",
            [AuthzedAction.DESCRIBE, AuthzedAction.UPDATE],
            False,
            [False, False],
            [True, True],
            True,
            True,
        ),
        (
            "admin",
            [AuthzedAction.DESCRIBE, AuthzedAction.UPDATE],
            False,
            [False, True],
            [True, False],
            True,
            False,
        ),
        (
            "admin",
            [AuthzedAction.DESCRIBE, AuthzedAction.UPDATE],
            False,
            [False, True],
            [True, False],
            True,
            True,
        ),
        (
            "admin",
            READ + [AuthzedAction.UPDATE],
            False,
            [False, False],
            [True, True],
            True,
            False,
        ),
        (
            "admin",
            READ + [AuthzedAction.UPDATE],
            False,
            [False, False],
            [True, True],
            True,
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
    if intra_communication_flag:
        monkeypatch.setenv("INTRA_COMMUNICATION_BASE64", "test1234")
    else:
        monkeypatch.delenv("INTRA_COMMUNICATION_BASE64", False)

    sm = security_manager
    resources = feature_views

    user = users.get(username)
    sm.set_current_user(user)

    result = []
    if raise_error_in_permit:
        with pytest.raises(PermissionError):
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
            with pytest.raises(PermissionError):
                assert_permissions(resource=r, actions=requested_actions)
        else:
            result = assert_permissions(resource=r, actions=requested_actions)
            assertpy.assert_that(result).is_none()
