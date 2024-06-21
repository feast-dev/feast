from unittest.mock import Mock

import assertpy
import pytest

from feast import (
    FeatureView,
)
from feast.permissions.authorized_resource import ALL_RESOURCE_TYPES
from feast.permissions.permission import (
    AuthzedAction,
    AuthzedResource,
    DecisionStrategy,
    Permission,
)
from feast.permissions.policy import AllowAll


def test_global_decision_strategy():
    assertpy.assert_that(Permission.get_global_decision_strategy()).is_equal_to(
        DecisionStrategy.UNANIMOUS
    )

    for value in DecisionStrategy.__members__.values():
        Permission.set_global_decision_strategy(value)
        assertpy.assert_that(Permission.get_global_decision_strategy()).is_equal_to(
            value
        )


def test_permission_defaults():
    p = Permission(name="test")
    assertpy.assert_that(type(p.resources)).is_equal_to(list)
    assertpy.assert_that(p.resources[0]).is_equal_to(ALL_RESOURCE_TYPES)
    assertpy.assert_that(type(p.actions)).is_equal_to(list)
    assertpy.assert_that(p.actions[0]).is_equal_to(AuthzedAction.ALL)
    assertpy.assert_that(type(p.actions)).is_equal_to(list)
    assertpy.assert_that(type(p.policies)).is_equal_to(list)
    assertpy.assert_that(p.policies[0]).is_equal_to(AllowAll)
    assertpy.assert_that(p.decision_strategy).is_equal_to(DecisionStrategy.UNANIMOUS)


def test_permission_validity():
    all_resources = AuthzedResource(type=ALL_RESOURCE_TYPES)
    # Invalid values
    with pytest.raises(ValueError):
        Permission(name="test", resources=None)
    with pytest.raises(ValueError):
        Permission(name="test", resources=[])
    with pytest.raises(ValueError):
        Permission(name="test", resources=all_resources, actions=None)
    with pytest.raises(ValueError):
        Permission(name="test", resources=all_resources, actions=[])
    with pytest.raises(ValueError):
        Permission(name="test", resources=all_resources, policies=None)
    with pytest.raises(ValueError):
        Permission(name="test", resources=all_resources, policies=[])
    with pytest.raises(ValueError):
        Permission(name="test", resources=all_resources, decision_strategy="invalid")

    # Valid values
    Permission(name="test", resources=AuthzedResource("ALL"))
    Permission(
        name="test", resources=[AuthzedResource("ALL"), AuthzedResource(FeatureView)]
    )
    Permission(name="test", resources=all_resources, actions=AuthzedAction.ALL)
    Permission(name="test", resources=all_resources, actions=[AuthzedAction.ALL])
    Permission(
        name="test",
        resources=all_resources,
        actions=[AuthzedAction.CREATE, AuthzedAction.DELETE],
    )


def test_permission_normalized_args():
    p = Permission(name="test", resources=AuthzedResource("ALL"))
    assertpy.assert_that(type(p.resources)).is_equal_to(list)
    assertpy.assert_that(p.resources).is_equal_to([AuthzedResource("ALL")])

    p = Permission(name="test", actions=AuthzedAction.CREATE)
    assertpy.assert_that(type(p.actions)).is_equal_to(list)
    assertpy.assert_that(p.actions).is_equal_to([AuthzedAction.CREATE])

    p = Permission(name="test", policies=Mock())
    assertpy.assert_that(type(p.policies)).is_equal_to(list)


def test_permission_match_actions():
    p = Permission(name="test", resources=AuthzedResource("ALL"))

    for action in AuthzedAction.__members__.values():
        assertpy.assert_that(p.match_actions(actions=action)).is_true()

    p = Permission(name="test", resources=AuthzedResource("ALL"))
