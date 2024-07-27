import assertpy
import pytest

from feast.permissions.action import QUERY, AuthzedAction
from feast.permissions.security_manager import assert_permissions, permitted_resources


@pytest.mark.parametrize(
    "username, requested_actions, allowed, allowed_single, raise_error_in_assert, raise_error_in_permit",
    [
        (None, [], False, [False, False], [True, True], False),
        ("r", [AuthzedAction.READ], False, [True, False], [True, True], False),
        ("r", [AuthzedAction.UPDATE], False, [False, False], [True, True], False),
        ("w", [AuthzedAction.READ], False, [False, False], [True, True], False),
        ("w", [AuthzedAction.UPDATE], False, [True, False], [True, True], False),
        ("rw", [AuthzedAction.READ], False, [True, False], [True, True], False),
        ("rw", [AuthzedAction.UPDATE], False, [True, False], [True, True], False),
        (
            "rw",
            [AuthzedAction.READ, AuthzedAction.UPDATE],
            False,
            [False, False],
            [True, True],
            True,
        ),
        (
            "admin",
            [AuthzedAction.READ, AuthzedAction.UPDATE],
            False,
            [False, True],
            [True, False],
            True,
        ),
        (
            "admin",
            QUERY + [AuthzedAction.UPDATE],
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
