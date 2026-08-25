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


def test_update_passes_each_backend_only_its_own_tables(repo_config, entity):
    """A backend must not create or drop infrastructure for another backend's views."""
    fv_redis = _feature_view("fv_redis", "redis", entity)
    fv_sqlite = _feature_view("fv_sqlite", "sqlite", entity)
    fv_redis_gone = _feature_view("fv_redis_gone", "redis", entity)

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
            tables_to_delete=[fv_redis_gone],
            tables_to_keep=[fv_redis, fv_sqlite],
            entities_to_delete=[],
            entities_to_keep=[entity],
            partial=False,
        )

    _, redis_delete, redis_keep, *_ = redis_update.call_args.args
    _, sqlite_delete, sqlite_keep, *_ = sqlite_update.call_args.args
    assert redis_keep == [fv_redis]
    assert redis_delete == [fv_redis_gone]
    assert sqlite_keep == [fv_sqlite]
    assert sqlite_delete == []


def test_update_reaches_a_backend_with_only_deletions(repo_config, entity):
    """A backend whose views are all being removed still needs its update() call."""
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
            tables_to_delete=[_feature_view("fv_redis_gone", "redis", entity)],
            tables_to_keep=[_feature_view("fv_sqlite", "sqlite", entity)],
            entities_to_delete=[],
            entities_to_keep=[entity],
            partial=False,
        )

    assert redis_update.call_count == 1
    assert sqlite_update.call_count == 1


def test_teardown_passes_each_backend_only_its_own_tables(repo_config, entity):
    """Teardown used to hand every backend the full table list."""
    fv_redis = _feature_view("fv_redis", "redis", entity)
    fv_sqlite = _feature_view("fv_sqlite", "sqlite", entity)
    fv_sqlite2 = _feature_view("fv_sqlite2", "sqlite", entity)

    with (
        patch(
            "feast.infra.online_stores.redis.RedisOnlineStore.teardown"
        ) as redis_teardown,
        patch(
            "feast.infra.online_stores.sqlite.SqliteOnlineStore.teardown"
        ) as sqlite_teardown,
    ):
        HybridOnlineStore().teardown(
            config=repo_config,
            tables=[fv_redis, fv_sqlite, fv_sqlite2],
            entities=[entity],
        )

    assert redis_teardown.call_count == 1
    assert sqlite_teardown.call_count == 1
    assert redis_teardown.call_args.args[1] == [fv_redis]
    # Both sqlite views in one call: the old dedup dropped the second one.
    assert sqlite_teardown.call_args.args[1] == [fv_sqlite, fv_sqlite2]
