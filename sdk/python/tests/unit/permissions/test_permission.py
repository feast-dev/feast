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
from feast.permissions.policy import AllowAll, Policy


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


@pytest.mark.parametrize(
    "dict, result",
    [
        ({"types": None}, False),
        ({"types": []}, False),
        ({"types": ALL_RESOURCE_TYPES}, True),
        ({"types": [FeatureView, FeatureService]}, True),
        ({"actions": None}, False),
        ({"actions": []}, False),
        ({"actions": AuthzedAction.ALL}, True),
        ({"actions": [AuthzedAction.ALL]}, True),
        ({"actions": [AuthzedAction.CREATE, AuthzedAction.DELETE]}, True),
        ({"policies": None}, False),
        ({"policies": []}, False),
        ({"policies": Mock(spec=Policy)}, True),
        ({"policies": [Mock(spec=Policy)]}, True),
        ({"decision_strategy": DecisionStrategy.AFFIRMATIVE}, True),
    ],
)
def test_validity(dict, result):
    if not result:
        with pytest.raises(ValueError):
            Permission(name="test", **dict)
    else:
        Permission(name="test", **dict)


def test_normalized_args():
    p = Permission(name="test")
    assertpy.assert_that(type(p.types)).is_equal_to(list)
    assertpy.assert_that(p.types).is_equal_to(ALL_RESOURCE_TYPES)

    p = Permission(name="test", actions=AuthzedAction.CREATE)
    assertpy.assert_that(type(p.actions)).is_equal_to(list)
    assertpy.assert_that(p.actions).is_equal_to([AuthzedAction.CREATE])

    p = Permission(name="test", policies=Mock())
    assertpy.assert_that(type(p.policies)).is_equal_to(list)


@pytest.mark.parametrize(
    "resource, result",
    [
        (None, False),
        ("invalid string", False),
        ("ALL", False),
        ("ALL", False),
    ]
    + [(Mock(spec=t), True) for t in get_args(FeastObject)],
)
def test_match_resource(resource, result):
    p = Permission(name="test")
    assertpy.assert_that(p.match_resource(resource)).is_equal_to(result)


@pytest.mark.parametrize(
    "pattern, name, match",
    [
        ("test.*", "test", True),
        ("test.*", "test1", True),
        ("test.*", "wrongtest", False),
        (".*test.*", "wrongtest", True),
    ],
)
def test_resource_match_with_name_filter(pattern, name, match):
    p = Permission(name="test", name_pattern=pattern)
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = name
        assertpy.assert_that(p.match_resource(resource)).is_equal_to(match)


@pytest.mark.parametrize(
    ("required_tags, tags, result"),
    [
        ({"owner": "dev"}, {}, False),
        ({"owner": "dev"}, {"owner": "master"}, False),
        ({"owner": "dev"}, {"owner": "dev", "other": 1}, True),
        ({"owner": "dev", "dep": 1}, {"owner": "dev", "other": 1}, False),
        ({"owner": "dev", "dep": 1}, {"owner": "dev", "other": 1, "dep": 1}, True),
    ],
)
def test_resource_match_with_tags(required_tags, tags, result):
    # Missing tags
    p = Permission(name="test", required_tags=required_tags)
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        resource.tags = tags
        assertpy.assert_that(p.match_resource(resource)).is_equal_to(result)


@pytest.mark.parametrize(
    ("permitted_actions, actions, result"),
    [(AuthzedAction.ALL, a, True) for a in AuthzedAction.__members__.values()]
    + [
        (
            [AuthzedAction.CREATE, AuthzedAction.DELETE],
            [AuthzedAction.CREATE, AuthzedAction.DELETE],
            True,
        ),
        ([AuthzedAction.CREATE, AuthzedAction.DELETE], [AuthzedAction.CREATE], True),
        ([AuthzedAction.CREATE, AuthzedAction.DELETE], [AuthzedAction.DELETE], True),
        ([AuthzedAction.CREATE, AuthzedAction.DELETE], [AuthzedAction.UPDATE], False),
        (
            [AuthzedAction.CREATE, AuthzedAction.DELETE],
            [AuthzedAction.CREATE, AuthzedAction.DELETE, AuthzedAction.UPDATE],
            False,
        ),
    ],
)
def test_match_actions(permitted_actions, actions, result):
    p = Permission(name="test", actions=permitted_actions)
    assertpy.assert_that(p.match_actions(actions=actions)).is_equal_to(result)
