import assertpy
import pytest

from feast.permissions.user import User


@pytest.fixture(scope="module")
def users():
    users = []
    users.append(User("a", ["a1", "a2"]))
    users.append(User("b", ["b1", "b2"]))
    return dict([(u.username, u) for u in users])


@pytest.mark.parametrize(
    "username, roles, result",
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
def test_user_has_matching_role(users, username, roles, result):
    user = users.get(username, User(username, []))
    assertpy.assert_that(user.has_matching_role(requested_roles=roles)).is_equal_to(
        result
    )
