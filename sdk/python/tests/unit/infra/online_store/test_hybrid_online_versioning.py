from datetime import datetime, timedelta
from unittest.mock import patch

import pytest

from feast import Entity, FeatureView, Field, FileSource, RepoConfig
from feast.errors import VersionedOnlineReadNotSupported
from feast.infra.online_stores.hybrid_online_store.hybrid_online_store import (
    HybridOnlineStore,
    HybridOnlineStoreConfig,
)
from feast.infra.online_stores.redis import RedisOnlineStore
from feast.infra.online_stores.sqlite import SqliteOnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Float32
from feast.value_type import ValueType


def _make_fv(tribe: str, version_number=None) -> FeatureView:
    fv = FeatureView(
        name="driver_stats",
        entities=[
            Entity(
                name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
            )
        ],
        schema=[Field(name="trips_today", dtype=Float32)],
        source=FileSource(path="dummy.parquet", timestamp_field="event_timestamp"),
        ttl=timedelta(days=1),
        tags={"tribe": tribe},
    )
    if version_number is not None:
        fv.current_version_number = version_number
    return fv


def _make_config(tmp_path, enable_versioning: bool) -> RepoConfig:
    return RepoConfig(
        project="test_project",
        provider="local",
        registry={
            "path": str(tmp_path / "registry.db"),
            "enable_online_feature_view_versioning": enable_versioning,
        },
        online_store=HybridOnlineStoreConfig(
            online_stores=[
                HybridOnlineStoreConfig.OnlineStoresWithConfig(
                    type="sqlite",
                    conf={"path": str(tmp_path / "online.db")},
                ),
                HybridOnlineStoreConfig.OnlineStoresWithConfig(
                    type="redis",
                    conf={
                        "redis_type": "redis",
                        "connection_string": "localhost:6379",
                    },
                ),
            ]
        ),
        offline_store=None,
        entity_key_serialization_version=3,
    )


def _entity_key(driver_id: int) -> EntityKeyProto:
    return EntityKeyProto(
        join_keys=["driver_id"], entity_values=[ValueProto(int64_val=driver_id)]
    )


class TestHybridVersionedReadSupport:
    def test_versioned_ref_does_not_raise_before_routing(self):
        """The hybrid store itself defers the support check to the routed backend."""
        fv = _make_fv("sqlite")
        fv.projection.version_tag = 2
        HybridOnlineStore()._check_versioned_read_support([(fv, [])])

    def test_online_read_raises_for_unsupported_backend(self, tmp_path):
        config = _make_config(tmp_path, enable_versioning=True)
        fv = _make_fv("redis")
        fv.projection.version_tag = 2

        store = HybridOnlineStore()
        with (
            patch.object(
                RedisOnlineStore, "_is_versioned_read_supported", return_value=False
            ),
            patch.object(RedisOnlineStore, "online_read", return_value=[]) as read,
        ):
            with pytest.raises(VersionedOnlineReadNotSupported) as exc_info:
                store.online_read(config, fv, [_entity_key(1)])
        assert "RedisOnlineStore" in str(exc_info.value)
        read.assert_not_called()

    def test_online_read_delegates_versioned_ref_to_supported_backend(self, tmp_path):
        config = _make_config(tmp_path, enable_versioning=True)
        fv = _make_fv("sqlite")
        fv.projection.version_tag = 2

        store = HybridOnlineStore()
        with patch.object(SqliteOnlineStore, "online_read", return_value=[]) as read:
            store.online_read(config, fv, [_entity_key(1)])
        read.assert_called_once()


class TestHybridVersionedWriteRead:
    def test_versioned_write_and_read_round_trip_through_sqlite(self, tmp_path):
        # HybridOnlineStore rewrites the RepoConfig it is given into the routed
        # backend's config, so build a fresh config for every call.
        def cfg() -> RepoConfig:
            return _make_config(tmp_path, enable_versioning=True)

        fv_v1 = _make_fv("sqlite", version_number=1)
        fv_v2 = _make_fv("sqlite", version_number=2)

        store = HybridOnlineStore()
        store.update(cfg(), [], [fv_v1], [], [], partial=False)
        store.update(cfg(), [], [fv_v2], [], [], partial=False)

        now = datetime.utcnow()
        store.online_write_batch(
            cfg(),
            fv_v1,
            [(_entity_key(1), {"trips_today": ValueProto(float_val=1.0)}, now, now)],
            progress=None,
        )
        store.online_write_batch(
            cfg(),
            fv_v2,
            [(_entity_key(1), {"trips_today": ValueProto(float_val=2.0)}, now, now)],
            progress=None,
        )

        read_v1 = _make_fv("sqlite")
        read_v1.projection.version_tag = 1
        read_v2 = _make_fv("sqlite")
        read_v2.projection.version_tag = 2

        res_v1 = store.online_read(cfg(), read_v1, [_entity_key(1)])
        res_v2 = store.online_read(cfg(), read_v2, [_entity_key(1)])

        assert res_v1[0][1]["trips_today"].float_val == 1.0
        assert res_v2[0][1]["trips_today"].float_val == 2.0
