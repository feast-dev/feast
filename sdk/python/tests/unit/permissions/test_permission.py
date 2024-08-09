from unittest.mock import Mock

import assertpy
import pytest

from feast.batch_feature_view import BatchFeatureView
from feast.data_source import DataSource
from feast.entity import Entity
from feast.feast_object import ALL_RESOURCE_TYPES
from feast.feature_service import FeatureService
from feast.feature_view import FeatureView
from feast.on_demand_feature_view import OnDemandFeatureView
from feast.permissions.action import ALL_ACTIONS, AuthzedAction
from feast.permissions.permission import (
    Permission,
)
from feast.permissions.policy import AllowAll, Policy
from feast.saved_dataset import ValidationReference
from feast.stream_feature_view import StreamFeatureView


def test_defaults():
    p = Permission(name="test")
    assertpy.assert_that(type(p.types)).is_equal_to(list)
    assertpy.assert_that(p.types).is_equal_to(ALL_RESOURCE_TYPES)
    assertpy.assert_that(p.with_subclasses).is_true()
    assertpy.assert_that(p.name_pattern).is_none()
    assertpy.assert_that(p.tags).is_none()
    assertpy.assert_that(type(p.actions)).is_equal_to(list)
    assertpy.assert_that(p.actions).is_equal_to(ALL_ACTIONS)
    assertpy.assert_that(type(p.actions)).is_equal_to(list)
    assertpy.assert_that(isinstance(p.policy, Policy)).is_true()
    assertpy.assert_that(p.policy).is_equal_to(AllowAll)


@pytest.mark.parametrize(
    "dict, result",
    [
        ({"types": None}, True),
        ({"types": []}, True),
        ({"types": ALL_RESOURCE_TYPES}, True),
        ({"types": [FeatureView, FeatureService]}, True),
        ({"actions": None}, False),
        ({"actions": []}, False),
        ({"actions": ALL_ACTIONS}, True),
        ({"actions": ALL_ACTIONS}, True),
        ({"actions": [AuthzedAction.CREATE, AuthzedAction.DELETE]}, True),
        ({"policy": None}, False),
        ({"policy": []}, False),
        ({"policy": Mock(spec=Policy)}, True),
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


@pytest.mark.parametrize(
    "resource, types, result",
    [
        (None, ALL_RESOURCE_TYPES, False),
        ("invalid string", ALL_RESOURCE_TYPES, False),
        ("ALL", ALL_RESOURCE_TYPES, False),
        ("ALL", ALL_RESOURCE_TYPES, False),
        (
            Mock(spec=FeatureView),
            [t for t in ALL_RESOURCE_TYPES if t not in [FeatureView]],
            False,
        ),
        (
            Mock(spec=OnDemandFeatureView),
            [t for t in ALL_RESOURCE_TYPES if t not in [OnDemandFeatureView]],
            False,
        ),  # OnDemandFeatureView is a BaseFeatureView
        (
            Mock(spec=BatchFeatureView),
            FeatureView,
            True,
        ),  # BatchFeatureView is a FeatureView
        (
            Mock(spec=BatchFeatureView),
            [t for t in ALL_RESOURCE_TYPES if t not in [FeatureView, BatchFeatureView]],
            False,
        ),
        (
            Mock(spec=StreamFeatureView),
            FeatureView,
            True,
        ),  # StreamFeatureView is a FeatureView
        (
            Mock(spec=StreamFeatureView),
            [
                t
                for t in ALL_RESOURCE_TYPES
                if t not in [FeatureView, StreamFeatureView]
            ],
            False,
        ),
        (
            Mock(spec=Entity),
            [t for t in ALL_RESOURCE_TYPES if t not in [Entity]],
            False,
        ),
        (
            Mock(spec=FeatureService),
            [t for t in ALL_RESOURCE_TYPES if t not in [FeatureService]],
            False,
        ),
        (
            Mock(spec=DataSource),
            [t for t in ALL_RESOURCE_TYPES if t not in [DataSource]],
            False,
        ),
        (
            Mock(spec=ValidationReference),
            [t for t in ALL_RESOURCE_TYPES if t not in [ValidationReference]],
            False,
        ),
        (
            Mock(spec=Permission),
            [t for t in ALL_RESOURCE_TYPES if t not in [Permission]],
            False,
        ),
    ]
    + [(Mock(spec=t), ALL_RESOURCE_TYPES, True) for t in ALL_RESOURCE_TYPES]
    + [(Mock(spec=t), [t], True) for t in ALL_RESOURCE_TYPES],
)
def test_match_resource_with_subclasses(resource, types, result):
    p = Permission(name="test", types=types, with_subclasses=True)
    assertpy.assert_that(p.match_resource(resource)).is_equal_to(result)


@pytest.mark.parametrize(
    "resource, types, result",
    [
        (None, ALL_RESOURCE_TYPES, False),
        ("invalid string", ALL_RESOURCE_TYPES, False),
        ("ALL", ALL_RESOURCE_TYPES, False),
        ("ALL", ALL_RESOURCE_TYPES, False),
        (
            Mock(spec=FeatureView),
            [t for t in ALL_RESOURCE_TYPES if t not in [FeatureView]],
            False,
        ),
        (
            Mock(spec=OnDemandFeatureView),
            [t for t in ALL_RESOURCE_TYPES if t not in [OnDemandFeatureView]],
            False,
        ),
        (
            Mock(spec=BatchFeatureView),
            [t for t in ALL_RESOURCE_TYPES if t not in [BatchFeatureView]],
            False,
        ),
        (
            Mock(spec=StreamFeatureView),
            [t for t in ALL_RESOURCE_TYPES if t not in [StreamFeatureView]],
            False,
        ),
        (
            Mock(spec=Entity),
            [t for t in ALL_RESOURCE_TYPES if t not in [Entity]],
            False,
        ),
        (
            Mock(spec=FeatureService),
            [t for t in ALL_RESOURCE_TYPES if t not in [FeatureService]],
            False,
        ),
        (
            Mock(spec=DataSource),
            [t for t in ALL_RESOURCE_TYPES if t not in [DataSource]],
            False,
        ),
        (
            Mock(spec=ValidationReference),
            [t for t in ALL_RESOURCE_TYPES if t not in [ValidationReference]],
            False,
        ),
        (
            Mock(spec=Permission),
            [t for t in ALL_RESOURCE_TYPES if t not in [Permission]],
            False,
        ),
    ]
    + [(Mock(spec=t), ALL_RESOURCE_TYPES, True) for t in ALL_RESOURCE_TYPES]
    + [(Mock(spec=t), [t], True) for t in ALL_RESOURCE_TYPES],
)
def test_match_resource_no_subclasses(resource, types, result):
    p = Permission(name="test", types=types, with_subclasses=False)
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
    for t in ALL_RESOURCE_TYPES:
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
    p = Permission(name="test", tags=required_tags)
    for t in ALL_RESOURCE_TYPES:
        resource = Mock(spec=t)
        resource.name = "test"
        resource.tags = tags
        assertpy.assert_that(p.match_resource(resource)).is_equal_to(result)


@pytest.mark.parametrize(
    ("permitted_actions, requested_actions, result"),
    [(ALL_ACTIONS, [a], True) for a in AuthzedAction.__members__.values()]
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
def test_match_actions(permitted_actions, requested_actions, result):
    p = Permission(name="test", actions=permitted_actions)
    assertpy.assert_that(
        p.match_actions(requested_actions=requested_actions)
    ).is_equal_to(result)
