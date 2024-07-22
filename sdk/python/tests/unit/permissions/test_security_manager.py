import assertpy
import pytest

from feast.permissions.action import QUERY, AuthzedAction
from feast.permissions.decision import DecisionStrategy
from feast.permissions.permission import Permission
from feast.permissions.security_manager import assert_permissions, permitted_resources


@pytest.fixture(scope="module", autouse=True)
def setup_module():
    Permission.set_global_decision_strategy(DecisionStrategy.UNANIMOUS)


@pytest.mark.parametrize(
    "username, requested_actions, allowed, allowed_single, raise_error",
    [
        (None, [], False, [False, False], [True, True]),
        ("r", [AuthzedAction.READ], False, [True, False], [True, True]),
        ("r", [AuthzedAction.UPDATE], False, [False, False], [True, True]),
        ("w", [AuthzedAction.READ], False, [False, False], [True, True]),
        ("w", [AuthzedAction.UPDATE], False, [True, False], [True, True]),
        ("rw", [AuthzedAction.READ], False, [True, False], [True, True]),
        ("rw", [AuthzedAction.UPDATE], False, [True, False], [True, True]),
        (
            "rw",
            [AuthzedAction.READ, AuthzedAction.UPDATE],
            False,
            [False, False],
            [True, True],
        ),
        (
            "admin",
            [AuthzedAction.READ, AuthzedAction.UPDATE],
            False,
            [False, True],
            [True, False],
        ),
        (
            "admin",
            QUERY + [AuthzedAction.UPDATE],
            False,
            [False, False],
            [True, True],
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
    raise_error,
):
    sm = security_manager
    resources = feature_views

    user = users.get(username)
    sm.set_current_user(user)
    result = permitted_resources(resources=resources, actions=requested_actions)
    if allowed:
        assertpy.assert_that(result).is_equal_to(resources)
    else:
        filtered = [r for i, r in enumerate(resources) if allowed_single[i]]
        assertpy.assert_that(result).is_equal_to(filtered)

    for i, r in enumerate(resources):
        if allowed_single[i]:
            result = assert_permissions(resource=r, actions=requested_actions)
            assertpy.assert_that(result).is_equal_to(r)
        elif raise_error[i]:
            with pytest.raises(PermissionError):
                assert_permissions(resource=r, actions=requested_actions)
        else:
            result = assert_permissions(resource=r, actions=requested_actions)
            assertpy.assert_that(result).is_none()
