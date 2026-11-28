"""Integration test: delete_table and teardown remove the sequence keys that
online_append writes to Redis, and only in their own project.

Run with: pytest --integration sdk/python/tests/integration/online_store/test_redis_sequence_cleanup.py
"""

import shutil
from datetime import datetime, timezone

import pytest

from feast import Entity, FeatureView, Field, FileSource, OnlineConfig
from feast.infra.online_stores.redis import RedisOnlineStore, RedisOnlineStoreConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import Int64
from feast.value_type import ValueType


@pytest.fixture
def redis_port():
    try:
        from testcontainers.redis import RedisContainer
    except ImportError:
        pytest.skip("testcontainers[redis] not installed")

    container = RedisContainer("redis:7").with_exposed_ports(6379)
    container.start()
    yield int(container.get_exposed_port(6379))
    container.stop()


def _config(project: str, port: int) -> RepoConfig:
    return RepoConfig(
        project=project,
        provider="local",
        online_store=RedisOnlineStoreConfig(connection_string=f"localhost:{port}"),
        registry="unused_registry.db",
        entity_key_serialization_version=3,
    )


def _sequence_view(name: str) -> FeatureView:
    driver = Entity(name="driver", join_keys=["driver_id"], value_type=ValueType.INT64)
    return FeatureView(
        name=name,
        entities=[driver],
        schema=[Field(name="driver_id", dtype=Int64), Field(name="trips", dtype=Int64)],
        source=FileSource(path="unused.parquet", timestamp_field="event_timestamp"),
        online_config=OnlineConfig(mode="sequence", write_mode="append"),
    )


def _append_events(store, config, view) -> None:
    entity_key = EntityKeyProto(
        join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1001)]
    )
    now = datetime.now(tz=timezone.utc)
    store.online_append(
        config,
        view,
        [
            (entity_key, {"trips": ValueProto(int64_val=trips)}, now, None)
            for trips in (1, 2)
        ],
    )


@pytest.mark.integration
@pytest.mark.skipif(not shutil.which("docker"), reason="Docker not available")
def test_delete_table_and_teardown_remove_sequence_keys(redis_port):
    import redis

    store = RedisOnlineStore()
    # other_demo ends with demo, so a scan for demo's keys must not match it.
    demo, other_demo = (
        _config("demo", redis_port),
        _config("other_demo", redis_port),
    )
    clicks, views = _sequence_view("clicks"), _sequence_view("views")
    _append_events(store, demo, clicks)
    _append_events(store, demo, views)
    _append_events(store, other_demo, clicks)

    client = redis.Redis(port=redis_port)

    def sequence_keys():
        # Key: seq:<project>:<feature view>:<entity hash key>
        labels = []
        for key in client.scan_iter(b"seq:*"):
            _, project, view, _ = key.split(b":", 3)
            labels.append(f"{view.decode()}@{project.decode()}")
        return sorted(labels)

    assert sequence_keys() == [
        "clicks@demo",
        "clicks@other_demo",
        "views@demo",
    ]

    # Deleting a view removes only its keys in its project.
    store.delete_table(demo, clicks)
    assert sequence_keys() == ["clicks@other_demo", "views@demo"]

    # Teardown removes the keys of the views it is given, in its project.
    store.teardown(demo, [views], [])
    assert sequence_keys() == ["clicks@other_demo"]
