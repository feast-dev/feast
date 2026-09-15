"""Unit tests for Cassandra online store feature view versioning."""

from datetime import timedelta
from unittest.mock import MagicMock

import pytest

# Skip the entire module when the optional cassandra-driver is not installed.
pytest.importorskip("cassandra", reason="cassandra-driver not installed")

from feast import Entity, FeatureView  # noqa: E402
from feast.field import Field  # noqa: E402
from feast.infra.online_stores.cassandra_online_store.cassandra_online_store import (  # noqa: E402
    CassandraOnlineStore,
)
from feast.types import Float32  # noqa: E402
from feast.value_type import ValueType  # noqa: E402

KEYSPACE = "feast_keyspace"
PROJECT = "test_project"


def _make_feature_view(name="driver_stats", version_number=None, version_tag=None):
    entity = Entity(
        name="driver_id",
        join_keys=["driver_id"],
        value_type=ValueType.INT64,
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


def _make_config(project=PROJECT, versioning=False):
    config = MagicMock()
    config.project = project
    config.entity_key_serialization_version = 3
    config.registry.enable_online_feature_view_versioning = versioning
    return config


class TestCassandraFqTableName:
    """_fq_table_name appends the version only when versioning is enabled."""

    def test_no_versioning(self):
        fv = _make_feature_view()
        assert (
            CassandraOnlineStore._fq_table_name(KEYSPACE, PROJECT, fv, False)
            == '"feast_keyspace"."test_project_driver_stats"'
        )

    def test_versioning_defaults_to_off(self):
        fv = _make_feature_view(version_number=2)
        assert (
            CassandraOnlineStore._fq_table_name(KEYSPACE, PROJECT, fv)
            == '"feast_keyspace"."test_project_driver_stats"'
        )

    def test_versioning_disabled_ignores_version(self):
        fv = _make_feature_view(version_number=3)
        assert (
            CassandraOnlineStore._fq_table_name(KEYSPACE, PROJECT, fv, False)
            == '"feast_keyspace"."test_project_driver_stats"'
        )

    def test_versioning_enabled_no_version_set(self):
        fv = _make_feature_view()
        assert (
            CassandraOnlineStore._fq_table_name(KEYSPACE, PROJECT, fv, True)
            == '"feast_keyspace"."test_project_driver_stats"'
        )

    def test_versioning_enabled_with_current_version_number(self):
        fv = _make_feature_view(version_number=2)
        assert (
            CassandraOnlineStore._fq_table_name(KEYSPACE, PROJECT, fv, True)
            == '"feast_keyspace"."test_project_driver_stats_v2"'
        )

    def test_projection_version_tag_takes_priority(self):
        fv = _make_feature_view(version_number=1, version_tag=3)
        assert (
            CassandraOnlineStore._fq_table_name(KEYSPACE, PROJECT, fv, True)
            == '"feast_keyspace"."test_project_driver_stats_v3"'
        )

    def test_version_zero_has_no_suffix(self):
        fv = _make_feature_view(version_number=0)
        assert (
            CassandraOnlineStore._fq_table_name(KEYSPACE, PROJECT, fv, True)
            == '"feast_keyspace"."test_project_driver_stats"'
        )

    def test_versions_get_separate_tables(self):
        v1 = _make_feature_view(version_number=1)
        v2 = _make_feature_view(version_number=2)
        assert CassandraOnlineStore._fq_table_name(
            KEYSPACE, PROJECT, v1, True
        ) != CassandraOnlineStore._fq_table_name(KEYSPACE, PROJECT, v2, True)


class TestCassandraVersionedReadSupport:
    """A version-qualified read no longer raises on Cassandra."""

    def test_allowed_with_version_tag(self):
        store = CassandraOnlineStore()
        fv = _make_feature_view()
        fv.projection.version_tag = 2
        # Should not raise VersionedOnlineReadNotSupported
        store._check_versioned_read_support([(fv, ["trips_today"])])

    def test_allowed_without_version_tag(self):
        store = CassandraOnlineStore()
        fv = _make_feature_view()
        store._check_versioned_read_support([(fv, ["trips_today"])])


class TestCassandraDropAllVersionTables:
    """Deleting a versioned feature view removes every one of its tables."""

    @staticmethod
    def _store_with_tables(table_names):
        store = CassandraOnlineStore()
        store._keyspace = KEYSPACE
        # The cache is a class attribute; give this instance one of its own.
        store._prepared_statements = {}
        session = MagicMock()
        session.execute.return_value = [
            MagicMock(table_name=name) for name in table_names
        ]
        store._get_session = MagicMock(return_value=session)
        return store, session

    def _dropped(self, session):
        return [
            call.args[0]
            for call in session.execute.call_args_list
            if isinstance(call.args[0], str) and call.args[0].startswith("DROP TABLE")
        ]

    def test_drops_base_and_every_version(self):
        store, session = self._store_with_tables(
            [
                "test_project_driver_stats",
                "test_project_driver_stats_v1",
                "test_project_driver_stats_v2",
            ]
        )
        store._drop_all_version_tables(
            _make_config(versioning=True), PROJECT, _make_feature_view()
        )
        dropped = self._dropped(session)
        assert len(dropped) == 3
        assert any('"test_project_driver_stats_v2"' in cql for cql in dropped)
        assert any('"test_project_driver_stats_v1"' in cql for cql in dropped)

    def test_leaves_other_feature_views_alone(self):
        store, session = self._store_with_tables(
            [
                "test_project_driver_stats",
                "test_project_driver_stats_v1",
                "test_project_driver_stats_extra",
                "test_project_driver_stats_extra_v1",
                "test_project_other_view_v1",
            ]
        )
        store._drop_all_version_tables(
            _make_config(versioning=True), PROJECT, _make_feature_view()
        )
        dropped = self._dropped(session)
        assert len(dropped) == 2
        assert not any("extra" in cql for cql in dropped)
        assert not any("other_view" in cql for cql in dropped)

    def test_teardown_without_versioning_drops_only_the_view(self):
        store, session = self._store_with_tables([])
        store.teardown(_make_config(versioning=False), [_make_feature_view()], [])
        dropped = self._dropped(session)
        assert dropped == [
            'DROP TABLE IF EXISTS "feast_keyspace"."test_project_driver_stats";'
        ]
