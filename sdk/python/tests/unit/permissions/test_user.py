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


def test_users_have_independent_default_permissions():
    first_user = User("first")
    second_user = User("second")

    first_user.roles.append("reader")
    first_user.groups.append("analytics")
    first_user.namespaces.append("production")

    assert second_user.roles == []
    assert second_user.groups == []
    assert second_user.namespaces == []
