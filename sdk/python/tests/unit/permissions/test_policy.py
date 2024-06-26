import assertpy
import pytest

from feast.permissions.policy import AllowAll, RoleBasedPolicy
from feast.permissions.role_manager import RoleManager


@pytest.mark.parametrize(
    "user, dict",
    [
        ("any", None),
        (None, None),
        ("any", {"other_arg": 1234}),
    ],
)
def test_allow_all(user, dict):
    if dict:
        assertpy.assert_that(AllowAll.validate_user(user, **dict)).is_true()
    else:
        assertpy.assert_that(AllowAll.validate_user(user)).is_true()


@pytest.fixture
def role_manager() -> RoleManager:
    rm = RoleManager()
    rm.add_roles_for_user("r", ["reader"])
    rm.add_roles_for_user("w", ["writer"])
    rm.add_roles_for_user("rw", ["reader", "writer"])
    return rm


@pytest.mark.parametrize(
    "required_roles, user, result",
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
def test_role_based_policy(role_manager, required_roles, user, result):
    rm = role_manager
    policy = RoleBasedPolicy(roles=required_roles)

    with pytest.raises(ValueError):
        policy.validate_user(user=user)
    with pytest.raises(ValueError):
        policy.validate_user(user=user, role_manager="wrong-type")

    validate_result, explain = policy.validate_user(user, role_manager=rm)
    assertpy.assert_that(validate_result).is_equal_to(result)

    if result is True:
        assertpy.assert_that(explain).is_equal_to("")
    else:
        assertpy.assert_that(len(explain)).is_greater_than(0)
