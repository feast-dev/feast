import assertpy

from feast.permissions.policy import AllowAll


def test_allow_all():
    policy = AllowAll

    assertpy.assert_that(policy.validate_user("any")).is_true()
    assertpy.assert_that(policy.validate_user(None, roles=["any"])).is_true()
    assertpy.assert_that(policy.validate_user("any", other_arg=1234)).is_true()
