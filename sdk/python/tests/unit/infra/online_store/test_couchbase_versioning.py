"""Unit tests for Couchbase online store feature view versioning."""

from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest

from feast import Entity, FeatureView
from feast.field import Field
from feast.types import Float32
from feast.value_type import ValueType

pytest.importorskip("couchbase")

from feast.infra.online_stores.couchbase_online_store.couchbase import (  # noqa: E402
    CouchbaseOnlineStore,
    _scope_and_collection,
)


def _make_feature_view(name="driver_stats", version_number=None, version_tag=None):
    entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    fv = FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[Field(name="trips_today", dtype=Float32)],
    )
    if version_number is not None:
        fv.current_version_number = version_number
    if version_tag is not None:
        fv.projection.version_tag = version_tag
    return fv


def _make_config(project="test_project", versioning=False):
    config = MagicMock()
    config.project = project
    config.entity_key_serialization_version = 2
    config.registry.enable_online_feature_view_versioning = versioning
    return config


class TestScopeAndCollection:
    def test_no_versioning(self):
        scope, collection = _scope_and_collection(
            _make_config(versioning=False), _make_feature_view()
        )
        assert scope == "test_project_driver_stats_scope"
        assert collection == "test_project_driver_stats_collection"

    def test_versioning_disabled_ignores_version(self):
        """A version on the feature view must not change names while the flag is off."""
        scope, collection = _scope_and_collection(
            _make_config(versioning=False), _make_feature_view(version_number=2)
        )
        assert scope == "test_project_driver_stats_scope"
        assert collection == "test_project_driver_stats_collection"

    def test_versioning_enabled_with_version(self):
        scope, collection = _scope_and_collection(
            _make_config(versioning=True), _make_feature_view(version_number=2)
        )
        assert scope == "test_project_driver_stats_v2_scope"
        assert collection == "test_project_driver_stats_v2_collection"

    def test_projection_version_tag_takes_priority(self):
        scope, _ = _scope_and_collection(
            _make_config(versioning=True),
            _make_feature_view(version_number=1, version_tag=3),
        )
        assert scope == "test_project_driver_stats_v3_scope"

    def test_version_zero_no_suffix(self):
        scope, _ = _scope_and_collection(
            _make_config(versioning=True), _make_feature_view(version_number=0)
        )
        assert scope == "test_project_driver_stats_scope"

    def test_versions_do_not_collide(self):
        config = _make_config(versioning=True)
        v1, _ = _scope_and_collection(config, _make_feature_view(version_number=1))
        v2, _ = _scope_and_collection(config, _make_feature_view(version_number=2))
        assert v1 != v2


class TestVersionedNamesReachCouchbase:
    """The resolved names must be what the store actually connects with."""

    @pytest.mark.parametrize(
        "versioning, expected_scope",
        [
            (False, "test_project_driver_stats_scope"),
            (True, "test_project_driver_stats_v2_scope"),
        ],
    )
    def test_update_creates_the_versioned_scope(self, versioning, expected_scope):
        store = CouchbaseOnlineStore()
        config = _make_config(versioning=versioning)
        fv = _make_feature_view(version_number=2)

        with patch.object(CouchbaseOnlineStore, "_get_conn") as get_conn:
            store.bucket = MagicMock()
            store.update(config, [], [fv], [], [], partial=False)

            get_conn.assert_called_once_with(
                config, expected_scope, expected_scope.replace("_scope", "_collection")
            )
            store.bucket.collections().create_scope.assert_called_once_with(
                expected_scope
            )

    def test_teardown_drops_the_versioned_scope(self):
        store = CouchbaseOnlineStore()
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=2)

        with patch.object(CouchbaseOnlineStore, "_get_conn"):
            store.bucket = MagicMock()
            store.teardown(config, [fv], [])

            store.bucket.collections().drop_scope.assert_called_once_with(
                "test_project_driver_stats_v2_scope"
            )


class TestVersionedReadSupport:
    def test_couchbase_is_allowlisted_for_versioned_reads(self):
        """Couchbase's online_read honours the contract, so it must not raise."""
        store = CouchbaseOnlineStore()
        store._versioned_read_supported = None
        assert store._is_versioned_read_supported() is True

    def test_versioned_ref_does_not_raise(self):
        store = CouchbaseOnlineStore()
        store._versioned_read_supported = None
        fv = _make_feature_view(version_tag=2)
        store._check_versioned_read_support([(fv, ["trips_today"])])
