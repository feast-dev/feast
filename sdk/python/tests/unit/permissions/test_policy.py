import assertpy
import pytest

from feast.permissions.policy import AllowAll, RoleBasedPolicy
from feast.permissions.user import User


@pytest.mark.parametrize(
    "username",
    [("r"), ("w"), ("rw"), ("missing")],
)
def test_allow_all(users, username):
    user = users.get(username, User(username, []))
    assertpy.assert_that(AllowAll.validate_user(user)).is_true()


@pytest.mark.parametrize(
    "required_roles, username, result",
    [
        (["reader"], "r", True),
        (["writer"], "r", False),
        (["reader", "writer"], "r", True),
        (["writer", "updater"], "r", False),
        (["reader"], "w", False),
        (["writer"], "w", True),
        (["reader", "writer"], "w", True),
        (["reader", "updater"], "w", False),
        (["reader"], "rw", True),
        (["writer"], "rw", True),
        (["reader", "writer"], "rw", True),
        (["updater"], "rw", False),
    ],
)
def test_role_based_policy(users, required_roles, username, result):
    user = users.get(username)
    policy = RoleBasedPolicy(roles=required_roles)

    validate_result, explain = policy.validate_user(user)
    assertpy.assert_that(validate_result).is_equal_to(result)

    if result is True:
        assertpy.assert_that(explain).is_equal_to("")
    else:
        assertpy.assert_that(len(explain)).is_greater_than(0)
