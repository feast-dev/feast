"""Unit tests for HBase online store feature view versioning."""

import struct
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest

from feast import Entity
from feast.feature_view import FeatureView
from feast.field import Field
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Float32
from feast.value_type import ValueType

pytest.importorskip("happybase")

from feast.infra.online_stores.hbase_online_store.hbase import (  # noqa: E402
    HbaseOnlineStore,
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
    config.entity_key_serialization_version = 3
    config.registry.enable_online_feature_view_versioning = versioning
    return config


class TestTableId:
    """HBase names tables `namespace:table`, so the version belongs on the table half."""

    def test_no_versioning(self):
        store = HbaseOnlineStore()
        assert store._table_id("test_project", _make_feature_view()) == (
            "test_project:driver_stats"
        )

    def test_versioning_disabled_ignores_version(self):
        store = HbaseOnlineStore()
        fv = _make_feature_view(version_number=2)
        assert store._table_id("test_project", fv, enable_versioning=False) == (
            "test_project:driver_stats"
        )

    def test_versioning_enabled_with_version(self):
        store = HbaseOnlineStore()
        fv = _make_feature_view(version_number=2)
        assert store._table_id("test_project", fv, enable_versioning=True) == (
            "test_project:driver_stats_v2"
        )

    def test_projection_version_tag_takes_priority(self):
        store = HbaseOnlineStore()
        fv = _make_feature_view(version_number=1, version_tag=3)
        assert store._table_id("test_project", fv, enable_versioning=True) == (
            "test_project:driver_stats_v3"
        )

    def test_version_zero_no_suffix(self):
        store = HbaseOnlineStore()
        fv = _make_feature_view(version_number=0)
        assert store._table_id("test_project", fv, enable_versioning=True) == (
            "test_project:driver_stats"
        )

    def test_namespace_separator_is_preserved(self):
        """`compute_table_id` would join with `_`; HBase requires `:`."""
        store = HbaseOnlineStore()
        fv = _make_feature_view(version_number=2)
        assert store._table_id("p", fv, enable_versioning=True).startswith("p:")


@pytest.fixture
def store_with_mock_connector():
    store = HbaseOnlineStore()
    with (
        patch.object(HbaseOnlineStore, "_get_conn"),
        patch(
            "feast.infra.online_stores.hbase_online_store.hbase.HBaseConnector"
        ) as connector_cls,
    ):
        yield store, connector_cls.return_value


class TestVersionedTableNamesReachHBase:
    """The resolved name must be the one the store actually operates on."""

    @pytest.mark.parametrize(
        "versioning, expected",
        [(False, "test_project:driver_stats"), (True, "test_project:driver_stats_v2")],
    )
    def test_write_targets_the_versioned_table(
        self, store_with_mock_connector, versioning, expected
    ):
        store, connector = store_with_mock_connector
        config = _make_config(versioning=versioning)
        fv = _make_feature_view(version_number=2)
        entity_key = EntityKeyProto(
            join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1)]
        )

        store.online_write_batch(
            config,
            fv,
            [
                (
                    entity_key,
                    {"trips_today": ValueProto(float_val=1.0)},
                    datetime(2024, 1, 1),
                    None,
                )
            ],
            None,
        )

        connector.batch.assert_called_once_with(expected)

    def test_read_targets_the_versioned_table(self, store_with_mock_connector):
        store, connector = store_with_mock_connector
        connector.rows.return_value = []
        fv = _make_feature_view(version_number=2)
        entity_key = EntityKeyProto(
            join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1)]
        )

        store.online_read(
            _make_config(versioning=True), fv, [entity_key], ["trips_today"]
        )

        assert connector.rows.call_args[0][0] == "test_project:driver_stats_v2"

    def test_update_creates_and_deletes_versioned_tables(
        self, store_with_mock_connector
    ):
        store, connector = store_with_mock_connector
        connector.check_if_table_exist.return_value = False
        keep, delete = (
            _make_feature_view(version_number=2),
            _make_feature_view(name="old_stats", version_number=1),
        )

        store.update(
            _make_config(versioning=True), [delete], [keep], [], [], partial=False
        )

        connector.create_table_with_default_cf.assert_called_once_with(
            "test_project:driver_stats_v2"
        )
        connector.delete_table.assert_called_once_with("test_project:old_stats_v1")

    def test_teardown_deletes_the_versioned_table(self, store_with_mock_connector):
        store, connector = store_with_mock_connector
        fv = _make_feature_view(version_number=2)

        store.teardown(_make_config(versioning=True), [fv], [])

        connector.delete_table.assert_called_once_with("test_project:driver_stats_v2")

    def test_row_keys_stay_unversioned(self, store_with_mock_connector):
        """The table is the namespace, so row keys need no version and stay stable."""
        store, connector = store_with_mock_connector
        connector.rows.return_value = []
        fv = _make_feature_view(version_number=2)
        entity_key = EntityKeyProto(
            join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1)]
        )

        store.online_read(
            _make_config(versioning=True), fv, [entity_key], ["trips_today"]
        )

        row_keys = connector.rows.call_args[1]["row_keys"]
        assert all(b"_v2" not in key for key in row_keys)
        assert all(key.endswith(b"#driver_stats") for key in row_keys)

    def test_write_and_read_agree_on_row_keys(self, store_with_mock_connector):
        """Versioning must not desynchronise the two paths."""
        store, connector = store_with_mock_connector
        connector.rows.return_value = []
        config = _make_config(versioning=True)
        fv = _make_feature_view(version_number=2)
        entity_key = EntityKeyProto(
            join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1)]
        )

        store.online_write_batch(
            config,
            fv,
            [
                (
                    entity_key,
                    {"trips_today": ValueProto(float_val=1.0)},
                    datetime(2024, 1, 1),
                    None,
                )
            ],
            None,
        )
        written_key = connector.batch.return_value.put.call_args[0][0]

        store.online_read(config, fv, [entity_key], ["trips_today"])
        read_key = connector.rows.call_args[1]["row_keys"][0]

        assert written_key == read_key


class TestVersionsAreIsolated:
    def test_two_versions_use_different_tables(self, store_with_mock_connector):
        store, connector = store_with_mock_connector
        config = _make_config(versioning=True)
        v1, v2 = (
            _make_feature_view(version_number=1),
            _make_feature_view(version_number=2),
        )

        assert store._table_id(config.project, v1, True) != store._table_id(
            config.project, v2, True
        )

    def test_event_ts_still_round_trips(self, store_with_mock_connector):
        """Guard the read decode path while the table name changes underneath it."""
        store, connector = store_with_mock_connector
        ts = datetime(2024, 1, 1, 12, 0, 0)
        packed = struct.pack(">L", int(ts.timestamp()))
        value = ValueProto(float_val=2.0)
        connector.rows.return_value = [
            (
                b"key#driver_stats",
                {
                    b"data:trips_today": value.SerializeToString(),
                    b"data:event_ts": packed,
                },
            )
        ]
        fv = _make_feature_view(version_number=2)
        entity_key = EntityKeyProto(
            join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1)]
        )

        result = store.online_read(
            _make_config(versioning=True), fv, [entity_key], ["trips_today"]
        )

        assert len(result) == 1
        assert result[0][1]["trips_today"].float_val == pytest.approx(2.0)
