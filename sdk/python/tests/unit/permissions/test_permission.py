from typing import get_args
from unittest.mock import Mock

import assertpy
import pytest

from feast import (
    FeatureService,
    FeatureView,
)
from feast.feast_object import FeastObject
from feast.permissions.permission import (
    ALL_RESOURCE_TYPES,
    AuthzedAction,
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


def test_defaults():
    p = Permission(name="test")
    assertpy.assert_that(type(p.types)).is_equal_to(list)
    assertpy.assert_that(p.types).is_equal_to(ALL_RESOURCE_TYPES)
    assertpy.assert_that(p.with_subclasses).is_false()
    assertpy.assert_that(p.name_pattern).is_none()
    assertpy.assert_that(p.required_tags).is_none()
    assertpy.assert_that(type(p.actions)).is_equal_to(list)
    assertpy.assert_that(p.actions[0]).is_equal_to(AuthzedAction.ALL)
    assertpy.assert_that(type(p.actions)).is_equal_to(list)
    assertpy.assert_that(type(p.policies)).is_equal_to(list)
    assertpy.assert_that(p.policies[0]).is_equal_to(AllowAll)
    assertpy.assert_that(p.decision_strategy).is_equal_to(DecisionStrategy.UNANIMOUS)


def test_validity():
    # Invalid values
    with pytest.raises(ValueError):
        Permission(name="test", types=None)
    with pytest.raises(ValueError):
        Permission(name="test", types=[])
    with pytest.raises(ValueError):
        Permission(name="test", actions=None)
    with pytest.raises(ValueError):
        Permission(name="test", actions=[])
    with pytest.raises(ValueError):
        Permission(name="test", policies=None)
    with pytest.raises(ValueError):
        Permission(name="test", policies=[])
    with pytest.raises(ValueError):
        Permission(name="test", decision_strategy="invalid")

    # Valid values
    Permission(name="test", types=ALL_RESOURCE_TYPES)
    Permission(name="test", types=[FeatureView, FeatureService])
    Permission(name="test", actions=AuthzedAction.ALL)
    Permission(name="test", actions=[AuthzedAction.ALL])
    Permission(
        name="test",
        actions=[AuthzedAction.CREATE, AuthzedAction.DELETE],
    )


def test_normalized_args():
    p = Permission(name="test")
    assertpy.assert_that(type(p.types)).is_equal_to(list)
    assertpy.assert_that(p.types).is_equal_to(ALL_RESOURCE_TYPES)

    p = Permission(name="test", actions=AuthzedAction.CREATE)
    assertpy.assert_that(type(p.actions)).is_equal_to(list)
    assertpy.assert_that(p.actions).is_equal_to([AuthzedAction.CREATE])

    p = Permission(name="test", policies=Mock())
    assertpy.assert_that(type(p.policies)).is_equal_to(list)


def test_match_resource():
    p = Permission(name="test")
    resource = None
    assertpy.assert_that(p.match_resource(resource)).is_false()
    resource = "invalid string"
    assertpy.assert_that(p.match_resource(resource)).is_false()
    resource = "ALL"
    assertpy.assert_that(p.match_resource(resource)).is_false()

    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        assertpy.assert_that(p.match_resource(resource)).is_true()


def test_resource_match_with_name_filter():
    p = Permission(name="test", name_pattern="test.*")
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        assertpy.assert_that(p.match_resource(resource)).is_true()

    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test1"
        assertpy.assert_that(p.match_resource(resource)).is_true()

    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "wrongtest"
        assertpy.assert_that(p.match_resource(resource)).is_false()

    p = Permission(name="test", name_pattern=".*test.*")
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "wrongtest"
        assertpy.assert_that(p.match_resource(resource)).is_true()


def test_resource_match_with_tags():
    # Missing tags
    p = Permission(name="test", required_tags={"owner": "dev"})
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        resource.tags = {}
        assertpy.assert_that(p.match_resource(resource)).is_false()

    # Wrong tag value
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        resource.tags = {"owner": "master"}
        assertpy.assert_that(p.match_resource(resource)).is_false()

    # All matching tags
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        resource.tags = {"owner": "dev", "other": 1}
        assertpy.assert_that(p.match_resource(resource)).is_true()

    # One missing tag
    p = Permission(name="test", required_tags={"owner": "dev", "dep": 1})
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        resource.tags = {"owner": "dev", "other": 1}
        assertpy.assert_that(p.match_resource(resource)).is_false()

    # All matching tags
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        resource.tags = {"owner": "dev", "other": 1, "dep": 1}
        assertpy.assert_that(p.match_resource(resource)).is_true()


def test_match_actions():
    p = Permission(name="test")

    for action in AuthzedAction.__members__.values():
        assertpy.assert_that(p.match_actions(actions=action)).is_true()

    p = Permission(name="test", actions=[AuthzedAction.CREATE, AuthzedAction.DELETE])
    assertpy.assert_that(
        p.match_actions(actions=[AuthzedAction.CREATE, AuthzedAction.DELETE])
    ).is_true()
    assertpy.assert_that(p.match_actions(actions=[AuthzedAction.CREATE])).is_true()
    assertpy.assert_that(p.match_actions(actions=[AuthzedAction.DELETE])).is_true()
