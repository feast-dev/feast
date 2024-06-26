import assertpy
import pytest

from feast.permissions.role_manager import RoleManager


@pytest.fixture(scope="module")
def role_manager():
    rm = RoleManager()
    rm.add_roles_for_user("a", ["a1", "a2"])
    rm.add_roles_for_user("b", ["b1", "b2"])
    return rm


@pytest.mark.parametrize(
    "user, roles, result",
    [
        ("c", [], False),
        ("a", ["b1"], False),
        ("a", ["a1", "b1"], True),
        ("a", ["a1"], True),
        ("a", ["a1", "a2"], True),
        ("a", ["a1", "a2", "a3"], True),
        ("b", ["a1", "a3"], False),
        ("b", ["a1", "b1"], True),
        ("b", ["b1", "b2"], True),
        ("b", ["b1", "b2", "b3"], True),
    ],
)
def test_user_has_matching_role(role_manager, user, roles, result):
    rm = role_manager
    assertpy.assert_that(rm.user_has_matching_role(user=user, roles=roles)).is_equal_to(
        result
    )
