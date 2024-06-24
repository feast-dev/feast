import assertpy
import pytest

from feast.permissions.role_manager import RoleManager


@pytest.mark.parametrize(
    "user, roles, result",
    [
        ("c", [], False),
        ("a", ["a1"], True),
        ("a", ["a1", "a2"], True),
        ("a", ["a1", "a2", "a3"], False),
        ("b", ["a1", "a3"], False),
        ("b", ["b1", "b2"], True),
    ],
)
def test_has_roles(user, roles, result):
    rm = RoleManager()
    rm.add_roles_for_user("a", ["a1", "a2"])
    rm.add_roles_for_user("b", ["b1", "b2"])
    assertpy.assert_that(rm.has_roles_for_user(user=user, roles=roles)).is_equal_to(
        result
    )
