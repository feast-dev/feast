from unittest.mock import patch

import pytest

from feast import Entity, FeatureView, Field, FileSource, RepoConfig, ValueType
from feast.infra.online_stores.hybrid_online_store.hybrid_online_store import (
    HybridOnlineStore,
    HybridOnlineStoreConfig,
)
from feast.types import PrimitiveFeastType

ROUTING_TAG = "backend"


@pytest.fixture
def entity():
    return Entity(name="id", join_keys=["id"], value_type=ValueType.INT64)


def _feature_view(name: str, backend: str, entity: Entity) -> FeatureView:
    return FeatureView(
        name=name,
        entities=[entity],
        schema=[Field(name="feature1", dtype=PrimitiveFeastType.INT64)],
        online=True,
        tags={ROUTING_TAG: backend},
        source=FileSource(
            path="/tmp/feast_hybrid_test.parquet",
            event_timestamp_column="event_timestamp",
        ),
    )


@pytest.fixture
def repo_config():
    return RepoConfig(
        registry="test-registry.db",
        project="test_project",
        provider="local",
        online_store=HybridOnlineStoreConfig(
            routing_tag=ROUTING_TAG,
            online_stores=[
                HybridOnlineStoreConfig.OnlineStoresWithConfig(
                    type="redis",
                    conf={"redis_type": "redis", "connection_string": "localhost:6379"},
                ),
                HybridOnlineStoreConfig.OnlineStoresWithConfig(
                    type="sqlite",
                    conf={"path": "/tmp/feast_hybrid_test.db"},
                ),
            ],
        ),
        offline_store=None,
    )


def test_prepare_repo_conf_does_not_mutate_caller_config(repo_config):
    """The selected backend's config must not leak back into the caller's config."""
    original_online_store = repo_config.online_store
    original_redis_conf = dict(repo_config.online_store.online_stores[0].conf)

    HybridOnlineStore()._prepare_repo_conf(repo_config, "redis")

    assert repo_config.online_store is original_online_store
    assert repo_config.online_store.routing_tag == ROUTING_TAG
    # `type` used to be injected into the caller's own conf dict.
    assert repo_config.online_store.online_stores[0].conf == original_redis_conf


def test_update_routes_every_feature_view(repo_config, entity):
    """Regression: routing used to break from the second FeatureView onwards.

    `update()` rebound `config` to the selected backend's RepoConfig, so the next
    iteration read `routing_tag` off a config that no longer had one. It fell back
    to the "tribe" default, found no such tag, and raised
    "FeatureView must have a 'tribe' tag to use HybridOnlineStore".
    """
    tables = [
        _feature_view("fv_redis", "redis", entity),
        _feature_view("fv_sqlite", "sqlite", entity),
    ]

    with (
        patch(
            "feast.infra.online_stores.redis.RedisOnlineStore.update"
        ) as redis_update,
        patch(
            "feast.infra.online_stores.sqlite.SqliteOnlineStore.update"
        ) as sqlite_update,
    ):
        HybridOnlineStore().update(
            config=repo_config,
            tables_to_delete=[],
            tables_to_keep=tables,
            entities_to_delete=[],
            entities_to_keep=[entity],
            partial=False,
        )

    assert redis_update.call_count == 1
    assert sqlite_update.call_count == 1
    assert repo_config.online_store.routing_tag == ROUTING_TAG
