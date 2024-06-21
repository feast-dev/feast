import assertpy

from feast.permissions.role_manager import RoleManager


def test_has_roles():
    rm = RoleManager()
    rm.add_roles_for_user("a", ["a1", "a2"])
    rm.add_roles_for_user("b", ["b1", "b2"])

    assertpy.assert_that(rm.has_roles_for_user("c", list())).is_false()
    assertpy.assert_that(rm.has_roles_for_user("a", ["a1"])).is_true()
    assertpy.assert_that(rm.has_roles_for_user("a", ["a1", "a2"])).is_true()
    assertpy.assert_that(rm.has_roles_for_user("a", ["a1", "a2", "a3"])).is_false()
    assertpy.assert_that(rm.has_roles_for_user("a", ["a1", "a3"])).is_false()
    assertpy.assert_that(rm.has_roles_for_user("b", ["a1", "a3"])).is_false()
    assertpy.assert_that(rm.has_roles_for_user("b", ["b1", "b2"])).is_true()
