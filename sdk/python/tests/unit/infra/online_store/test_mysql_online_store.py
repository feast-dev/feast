"""Unit tests for the MySQL online store write paths (no live database)."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from feast import Entity, FeatureView
from feast.field import Field
from feast.infra.online_stores.mysql_online_store.mysql import (
    MySQLOnlineStore,
    MySQLOnlineStoreConfig,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import Float32, Int64
from feast.value_type import ValueType


def _make_feature_view(name: str = "driver_stats") -> FeatureView:
    entity = Entity(
        name="driver_id",
        join_keys=["driver_id"],
        value_type=ValueType.INT64,
    )
    return FeatureView(
        name=name,
        entities=[entity],
        ttl=timedelta(days=1),
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="trips_today", dtype=Int64),
            Field(name="avg_rating", dtype=Float32),
        ],
    )


def _make_entity_key(driver_id: int) -> EntityKeyProto:
    entity_key = EntityKeyProto()
    entity_key.join_keys.append("driver_id")
    val = ValueProto()
    val.int64_val = driver_id
    entity_key.entity_values.append(val)
    return entity_key


def _repo_config(online_store_cfg: MySQLOnlineStoreConfig) -> RepoConfig:
    return RepoConfig(
        registry="dummy_registry.db",
        project="test_project",
        provider="local",
        entity_key_serialization_version=3,
        online_store=online_store_cfg,
    )


def _write_data(n: int) -> list:
    now = datetime.now(tz=timezone.utc)
    data = []
    for i in range(n):
        val = ValueProto()
        val.int64_val = i
        data.append((_make_entity_key(1000 + i), {"trips_today": val}, now, now))
    return data


class TestMySQLBatchWrite:
    """The ``batch_write`` path of ``online_write_batch`` (no live database)."""

    def test_batch_write_reads_batch_size_from_config(self):
        """``batch_write`` must read the ``batch_size`` config field. It was
        previously misspelled ``bacth_size``, which raised ``AttributeError`` on
        the pydantic config and made the whole batch-write path unusable."""
        store = MySQLOnlineStore()
        config = _repo_config(
            MySQLOnlineStoreConfig(type="mysql", batch_write=True, batch_size=2)
        )
        with (
            patch.object(store, "_get_conn", return_value=MagicMock()),
            patch.object(store, "_execute_batch") as mock_execute_batch,
        ):
            store.online_write_batch(config, _make_feature_view(), _write_data(3), None)

        assert mock_execute_batch.called

    def test_batch_write_uses_same_entity_key_version_as_read(self):
        """The batch path must serialize entity keys with the same version the
        single-row write path and ``online_read`` use (3), otherwise
        batch-written rows cannot be read back."""
        store = MySQLOnlineStore()
        config = _repo_config(
            MySQLOnlineStoreConfig(type="mysql", batch_write=True, batch_size=2)
        )
        with (
            patch.object(store, "_get_conn", return_value=MagicMock()),
            patch.object(store, "_execute_batch"),
            patch(
                "feast.infra.online_stores.mysql_online_store.mysql.serialize_entity_key",
                return_value=b"\x00",
            ) as mock_serialize,
        ):
            store.online_write_batch(config, _make_feature_view(), _write_data(2), None)

        versions = {
            call.kwargs["entity_key_serialization_version"]
            for call in mock_serialize.call_args_list
        }
        assert versions == {3}
