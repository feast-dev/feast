from typing import get_args
from unittest.mock import Mock

import assertpy
import pytest

from feast import (
    FeatureStore,
)
from feast.feast_object import FeastObject
from feast.permissions.authorized_resource import (
    ALL_RESOURCE_TYPES,
    AuthzedResource,
)


def test_authorized_resource_types():
    # Invalid types
    with pytest.raises(ValueError):
        AuthzedResource("any")
    with pytest.raises(ValueError):
        AuthzedResource(FeatureStore)

    # Valid types
    AuthzedResource(ALL_RESOURCE_TYPES)
    for t in get_args(FeastObject):
        AuthzedResource(t)


def test_authorized_resource_type_defaults():
    authzed_resource = AuthzedResource(ALL_RESOURCE_TYPES)
    assertpy.assert_that(authzed_resource.with_subclasses).is_false()
    assertpy.assert_that(authzed_resource.name_pattern).is_none()
    assertpy.assert_that(authzed_resource.required_tags).is_none()


def test_authorized_resource_matches_ALL():
    authzed_resource = AuthzedResource(ALL_RESOURCE_TYPES)
    resource = None
    assertpy.assert_that(authzed_resource.match_resource(resource)).is_false()
    resource = "invalid string"
    assertpy.assert_that(authzed_resource.match_resource(resource)).is_false()
    resource = "ALL"
    assertpy.assert_that(authzed_resource.match_resource(resource)).is_false()

    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        assertpy.assert_that(authzed_resource.match_resource(resource)).is_true()


def test_authorized_resource_matches_ALL_with_name_filter():
    authzed_resource = AuthzedResource(ALL_RESOURCE_TYPES, name_pattern="test.*")
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        assertpy.assert_that(authzed_resource.match_resource(resource)).is_true()

    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test1"
        assertpy.assert_that(authzed_resource.match_resource(resource)).is_true()

    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "wrongtest"
        assertpy.assert_that(authzed_resource.match_resource(resource)).is_false()

    authzed_resource = AuthzedResource(ALL_RESOURCE_TYPES, name_pattern=".*test.*")
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "wrongtest"
        assertpy.assert_that(authzed_resource.match_resource(resource)).is_true()


def test_authorized_resource_matches_ALL_with_tags():
    # Missing tags
    authzed_resource = AuthzedResource(
        ALL_RESOURCE_TYPES, required_tags={"owner": "dev"}
    )
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        resource.tags = {}
        assertpy.assert_that(authzed_resource.match_resource(resource)).is_false()

    # Wrong tag value
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        resource.tags = {"owner": "master"}
        assertpy.assert_that(authzed_resource.match_resource(resource)).is_false()

    # All matching tags
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        resource.tags = {"owner": "dev", "other": 1}
        assertpy.assert_that(authzed_resource.match_resource(resource)).is_true()

    # One missing tag
    authzed_resource = AuthzedResource(
        ALL_RESOURCE_TYPES, required_tags={"owner": "dev", "dep": 1}
    )
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        resource.tags = {"owner": "dev", "other": 1}
        assertpy.assert_that(authzed_resource.match_resource(resource)).is_false()

    # All matching tags
    for t in get_args(FeastObject):
        resource = Mock(spec=t)
        resource.name = "test"
        resource.tags = {"owner": "dev", "other": 1, "dep": 1}
        assertpy.assert_that(authzed_resource.match_resource(resource)).is_true()
