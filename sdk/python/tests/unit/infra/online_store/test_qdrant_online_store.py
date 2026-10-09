from datetime import datetime, timedelta

import pytest

from feast import Entity, FeatureView, Field, FileSource, RepoConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Int64, String
from feast.value_type import ValueType

qdrant = pytest.importorskip("qdrant_client")

from feast.infra.online_stores.qdrant_online_store.qdrant import (  # noqa: E402
    QdrantOnlineStore,
    QdrantOnlineStoreConfig,
    _collection_name,
)


def _config(versioning: bool) -> RepoConfig:
    return RepoConfig(
        project="test_project",
        online_store=QdrantOnlineStoreConfig(
            type="qdrant",
            location=":memory:",
            vector_enabled=True,
            similarity="cosine",
        ),
        registry={
            "path": "dummy_registry",
            "enable_online_feature_view_versioning": versioning,
        },
        entity_key_serialization_version=3,
    )


def _feature_view(version: int = 0) -> FeatureView:
    fv = FeatureView(
        name="driver_stats",
        entities=[Entity(name="driver_id", value_type=ValueType.INT64)],
        ttl=timedelta(days=1),
        schema=[
            Field(name="driver_id", dtype=Int64),
            Field(name="trips", dtype=String),
        ],
        source=FileSource(path="driver.parquet", timestamp_field="event_timestamp"),
    )
    if version:
        fv.projection.version_tag = version
    return fv


def _write(store, config, fv, value):
    entity_key = EntityKeyProto(
        join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1)]
    )
    store.online_write_batch(
        config,
        fv,
        [
            (
                entity_key,
                {"trips": ValueProto(string_val=value)},
                datetime(2024, 1, 1),
                None,
            )
        ],
        None,
    )


class TestQdrantCollectionNaming:
    def test_unversioned_name_is_unchanged(self):
        """Existing deployments must keep their current collection names."""
        assert _collection_name(_config(False), _feature_view()) == "driver_stats"

    def test_versioning_disabled_ignores_the_version_tag(self):
        assert (
            _collection_name(_config(False), _feature_view(version=2)) == "driver_stats"
        )

    def test_versioned_name_gets_a_suffix(self):
        assert (
            _collection_name(_config(True), _feature_view(version=2))
            == "driver_stats_v2"
        )


class TestQdrantVersionedCollections:
    """End-to-end against an in-process Qdrant."""

    def test_versions_write_to_separate_collections(self):
        config = _config(True)
        store = QdrantOnlineStore()
        v1, v2 = _feature_view(version=1), _feature_view(version=2)

        store.update(config, [], [v1, v2], [], [], partial=False)
        names = {
            c.name for c in store._get_client(config).get_collections().collections
        }
        assert {"driver_stats_v1", "driver_stats_v2"} <= names

        _write(store, config, v1, "from_v1")
        _write(store, config, v2, "from_v2")

        client = store._get_client(config)
        assert client.count("driver_stats_v1").count == 1
        assert client.count("driver_stats_v2").count == 1

        payload = client.scroll("driver_stats_v1", limit=10, with_payload=True)[0]
        assert [p.payload["feature_name"] for p in payload] == ["trips"]

    def test_teardown_removes_only_the_targeted_version(self):
        config = _config(True)
        store = QdrantOnlineStore()
        v1, v2 = _feature_view(version=1), _feature_view(version=2)
        store.update(config, [], [v1, v2], [], [], partial=False)

        store.teardown(config, [v1], [])

        names = {
            c.name for c in store._get_client(config).get_collections().collections
        }
        assert "driver_stats_v1" not in names
        assert "driver_stats_v2" in names

    def test_unversioned_store_still_round_trips(self):
        """The default path must be untouched by the versioning change."""
        config = _config(False)
        store = QdrantOnlineStore()
        fv = _feature_view()

        store.update(config, [], [fv], [], [], partial=False)
        _write(store, config, fv, "value")

        client = store._get_client(config)
        assert client.count("driver_stats").count == 1
