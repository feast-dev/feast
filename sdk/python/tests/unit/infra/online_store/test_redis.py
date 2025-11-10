import time
from datetime import datetime, timedelta

import pytest
from google.protobuf.timestamp_pb2 import Timestamp
from redis import Redis

from feast import Entity, FeatureView, Field, FileSource, RepoConfig, ValueType
from feast.infra.online_stores.helpers import _mmh3, _redis_key
from feast.infra.online_stores.redis import (
    RedisOnlineStore,
    RedisOnlineStoreConfig,
)
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.sorted_feature_view import SortedFeatureView, SortKey
from feast.types import (
    Float32,
    Int32,
    UnixTimestamp,
)
from tests.unit.infra.online_store.redis_online_store_creator import (
    RedisOnlineStoreCreator,
)


@pytest.fixture
def redis_online_store() -> RedisOnlineStore:
    return RedisOnlineStore()


@pytest.fixture(scope="session")
def redis_online_store_config():
    creator = RedisOnlineStoreCreator("redis_project")
    config = creator.create_online_store()
    yield config
    creator.teardown()


@pytest.fixture
def repo_config(redis_online_store_config):
    return RepoConfig(
        provider="local",
        project="test",
        online_store=RedisOnlineStoreConfig(
            connection_string=redis_online_store_config["connection_string"],
        ),
        entity_key_serialization_version=3,
        registry="dummy_registry.db",
    )


@pytest.fixture
def feature_view():
    file_source = FileSource(name="my_file_source", path="test.parquet")
    entity = Entity(name="entity", join_keys=["entity"])
    feature_view = FeatureView(
        name="feature_view_1",
        entities=[entity],
        schema=[
            Field(name="feature_10", dtype=Int32),
            Field(name="feature_11", dtype=Int32),
            Field(name="feature_12", dtype=Int32),
        ],
        source=file_source,
    )
    return feature_view


def test_generate_entity_redis_keys(redis_online_store: RedisOnlineStore, repo_config):
    entity_keys = [
        EntityKeyProto(join_keys=["entity"], entity_values=[ValueProto(int32_val=1)]),
    ]

    actual = redis_online_store._generate_redis_keys_for_entities(
        repo_config, entity_keys
    )
    expected = [
        b"\x01\x00\x00\x00\x02\x00\x00\x00\x06\x00\x00\x00entity\x03\x00\x00\x00\x04\x00\x00\x00\x01\x00\x00\x00test"
    ]
    assert actual == expected


def test_generate_hset_keys_for_features(
    redis_online_store: RedisOnlineStore, feature_view
):
    actual = redis_online_store._generate_hset_keys_for_features(feature_view)
    expected = (
        ["feature_10", "feature_11", "feature_12", "_ts:feature_view_1"],
        [b"&m_9", b"\xc37\x9a\xbf", b"wr\xb5d", "_ts:feature_view_1"],
    )
    assert actual == expected


def test_generate_hset_keys_for_features_with_requested_features(
    redis_online_store: RedisOnlineStore, feature_view
):
    actual = redis_online_store._generate_hset_keys_for_features(
        feature_view=feature_view, requested_features=["my-feature-view:feature1"]
    )
    expected = (
        ["my-feature-view:feature1", "_ts:feature_view_1"],
        [b"Si\x86J", "_ts:feature_view_1"],
    )
    assert actual == expected


def test_convert_redis_values_to_protobuf(
    redis_online_store: RedisOnlineStore, feature_view
):
    requested_features = [
        "feature_view_1:feature_10",
        "feature_view_1:feature_11",
        "_ts:feature_view_1",
    ]
    values = [
        [
            ValueProto(int32_val=1).SerializeToString(),
            ValueProto(int32_val=2).SerializeToString(),
            Timestamp().SerializeToString(),
        ]
    ]

    features = redis_online_store._convert_redis_values_to_protobuf(
        redis_values=values,
        feature_view=feature_view.name,
        requested_features=requested_features,
    )
    assert isinstance(features, list)
    assert len(features) == 1

    timestamp, features = features[0]
    assert features["feature_view_1:feature_10"].int32_val == 1
    assert features["feature_view_1:feature_11"].int32_val == 2


def test_get_features_for_entity(redis_online_store: RedisOnlineStore, feature_view):
    requested_features = [
        "feature_view_1:feature_10",
        "feature_view_1:feature_11",
        "_ts:feature_view_1",
    ]
    values = [
        ValueProto(int32_val=1).SerializeToString(),
        ValueProto(int32_val=2).SerializeToString(),
        Timestamp().SerializeToString(),
    ]

    timestamp, features = redis_online_store._get_features_for_entity(
        values=values,
        feature_view=feature_view.name,
        requested_features=requested_features,
    )
    assert "feature_view_1:feature_10" in features
    assert "feature_view_1:feature_11" in features
    assert features["feature_view_1:feature_10"].int32_val == 1
    assert features["feature_view_1:feature_11"].int32_val == 2


def test_redis_online_write_batch_with_timestamp_as_sortkey(
    repo_config: RepoConfig,
    redis_online_store: RedisOnlineStore,
):
    (
        feature_view,
        data,
    ) = _create_sorted_feature_view_with_timestamp_as_sortkey()

    redis_online_store.online_write_batch(
        config=repo_config,
        table=feature_view,
        data=data,
        progress=None,
    )

    connection_string = repo_config.online_store.connection_string
    connection_string_split = connection_string.split(":")
    conn_dict = {}
    conn_dict["host"] = connection_string_split[0]
    conn_dict["port"] = connection_string_split[1]

    r = Redis(**conn_dict)

    pipe = r.pipeline(transaction=True)

    entity_key_driver_1 = EntityKeyProto(
        join_keys=["driver_id"],
        entity_values=[ValueProto(int32_val=1)],
    )

    redis_key_bin_driver_1 = _redis_key(
        repo_config.project,
        entity_key_driver_1,
        entity_key_serialization_version=repo_config.entity_key_serialization_version,
    )

    zset_key_driver_1 = f"{repo_config.project}:{feature_view.name}:{feature_view.sort_keys[0].name}:{redis_key_bin_driver_1}"

    entity_key_driver_2 = EntityKeyProto(
        join_keys=["driver_id"],
        entity_values=[ValueProto(int32_val=2)],
    )
    redis_key_bin_driver_2 = _redis_key(
        repo_config.project,
        entity_key_driver_2,
        entity_key_serialization_version=repo_config.entity_key_serialization_version,
    )

    zset_key_driver_2 = f"{repo_config.project}:{feature_view.name}:{feature_view.sort_keys[0].name}:{redis_key_bin_driver_2}"

    driver_1_zset_members = r.zrange(zset_key_driver_1, 0, -1, withscores=True)
    driver_2_zset_members = r.zrange(zset_key_driver_2, 0, -1, withscores=True)

    assert len(driver_1_zset_members) == 5
    assert len(driver_2_zset_members) == 5

    # Get last 3 trips for both drivers from the respective sorted sets
    last_3_trips_driver_1 = r.zrevrangebyscore(
        zset_key_driver_1, "+inf", "-inf", start=0, num=3
    )
    last_3_trips_driver_2 = r.zrevrangebyscore(
        zset_key_driver_2, "+inf", "-inf", start=0, num=3
    )

    # Look up features for last 3 trips for driver 1
    for id in last_3_trips_driver_1:
        pipe.hgetall(id)

    # Look up features for last 3 trips for driver 2
    for id in last_3_trips_driver_2:
        pipe.hgetall(id)

    features_list = pipe.execute()

    trip_id_feature_name = _mmh3(f"{feature_view.name}:trip_id")
    trip_id_drivers = []
    for feature_dict in features_list:
        val = ValueProto()
        val.ParseFromString(feature_dict[trip_id_feature_name])
        trip_id_drivers.append(val.int32_val)
    assert trip_id_drivers == [4, 3, 2, 9, 8, 7]


def test_redis_online_write_batch_with_float_as_sortkey(
    repo_config: RepoConfig,
    redis_online_store: RedisOnlineStore,
):
    (
        feature_view,
        data,
    ) = _create_sorted_feature_view_with_float_as_sortkey()

    redis_online_store.online_write_batch(
        config=repo_config,
        table=feature_view,
        data=data,
        progress=None,
    )

    connection_string = repo_config.online_store.connection_string
    connection_string_split = connection_string.split(":")
    conn_dict = {}
    conn_dict["host"] = connection_string_split[0]
    conn_dict["port"] = connection_string_split[1]

    r = Redis(**conn_dict)

    pipe = r.pipeline(transaction=True)

    entity_key_driver_1 = EntityKeyProto(
        join_keys=["driver_id"],
        entity_values=[ValueProto(int32_val=1)],
    )

    redis_key_bin_driver_1 = _redis_key(
        repo_config.project,
        entity_key_driver_1,
        entity_key_serialization_version=repo_config.entity_key_serialization_version,
    )

    zset_key_driver_1 = f"{repo_config.project}:{feature_view.name}:{feature_view.sort_keys[0].name}:{redis_key_bin_driver_1}"

    entity_key_driver_2 = EntityKeyProto(
        join_keys=["driver_id"],
        entity_values=[ValueProto(int32_val=2)],
    )
    redis_key_bin_driver_2 = _redis_key(
        repo_config.project,
        entity_key_driver_2,
        entity_key_serialization_version=repo_config.entity_key_serialization_version,
    )

    zset_key_driver_2 = f"{repo_config.project}:{feature_view.name}:{feature_view.sort_keys[0].name}:{redis_key_bin_driver_2}"

    driver_1_zset_members = r.zrange(zset_key_driver_1, 0, -1, withscores=True)
    driver_2_zset_members = r.zrange(zset_key_driver_2, 0, -1, withscores=True)

    assert len(driver_1_zset_members) == 5
    assert len(driver_2_zset_members) == 5

    # Get trips for driver 1 where ratings between 2.5 and 4.5
    # Get trips for driver 2 where ratings between 7.5 and 9.5
    driver_1_trips = r.zrangebyscore(zset_key_driver_1, 2.5, 4.5)
    driver_2_trips = r.zrangebyscore(zset_key_driver_2, 7.5, 9.5)

    # Look up features for trips for driver 1
    for id in driver_1_trips:
        pipe.hgetall(id)

    # Look up features for trips for driver 2
    for id in driver_2_trips:
        pipe.hgetall(id)

    features_list = pipe.execute()

    trip_id_feature_name = _mmh3(f"{feature_view.name}:trip_id")
    trip_id_drivers = []
    for feature_dict in features_list:
        val = ValueProto()
        val.ParseFromString(feature_dict[trip_id_feature_name])
        trip_id_drivers.append(val.int32_val)
    assert trip_id_drivers == [2, 3, 4, 7, 8, 9]


def _create_sorted_feature_view_with_timestamp_as_sortkey():
    fv = SortedFeatureView(
        name="driver_stats",
        source=FileSource(
            name="my_file_source",
            path="test.parquet",
            timestamp_field="event_timestamp",
        ),
        entities=[Entity(name="driver_id")],
        ttl=timedelta(seconds=10),
        sort_keys=[
            SortKey(
                name="event_timestamp",
                value_type=ValueType.UNIX_TIMESTAMP,
                default_sort_order=SortOrder.DESC,
            )
        ],
        schema=[
            Field(
                name="driver_id",
                dtype=Int32,
            ),
            Field(name="event_timestamp", dtype=UnixTimestamp),
            Field(
                name="trip_id",
                dtype=Int32,
            ),
            Field(
                name="rating",
                dtype=Float32,
            ),
        ],
    )

    return fv, _make_rows()


def _create_sorted_feature_view_with_float_as_sortkey(n=10):
    fv = SortedFeatureView(
        name="driver_stats",
        source=FileSource(
            name="my_file_source",
            path="test.parquet",
            timestamp_field="event_timestamp",
        ),
        entities=[Entity(name="driver_id")],
        ttl=timedelta(seconds=10),
        sort_keys=[
            SortKey(
                name="rating",
                value_type=ValueType.FLOAT,
                default_sort_order=SortOrder.DESC,
            )
        ],
        schema=[
            Field(
                name="driver_id",
                dtype=Int32,
            ),
            Field(name="event_timestamp", dtype=UnixTimestamp),
            Field(
                name="trip_id",
                dtype=Int32,
            ),
            Field(
                name="rating",
                dtype=Float32,
            ),
        ],
    )

    return fv, _make_rows()


def _make_rows(n=10):
    """Generate 10 rows split between driver_id 1 (first 5) and 2 (rest),
    with rating = i + 0.5 and an event_timestamp spanning ~15 minutes."""
    return [
        (
            EntityKeyProto(
                join_keys=["driver_id"],
                entity_values=[
                    ValueProto(int32_val=1) if i <= 4 else ValueProto(int32_val=2)
                ],
            ),
            {
                "trip_id": ValueProto(int32_val=i),
                "rating": ValueProto(float_val=i + 0.5),
                "event_timestamp": ValueProto(
                    unix_timestamp_val=int(
                        (
                            (datetime.utcnow() - timedelta(minutes=15))
                            + timedelta(minutes=i)
                        ).timestamp()
                    )
                ),
            },
            datetime.utcnow(),
            None,
        )
        for i in range(n)
    ]


def _make_redis_client(repo_config):
    connection_string = repo_config.online_store.connection_string
    host, port = connection_string.split(":")
    return Redis(host=host, port=int(port), decode_responses=True)


def test_ttl_cleanup_removes_expired_members_and_index(repo_config):
    """Ensure TTL cleanup removes expired members, hashes, and deletes empty ZSETs."""
    redis_client = _make_redis_client(repo_config)
    store = RedisOnlineStore()
    zset_key = "test:ttl_cleanup:zset"
    ttl_seconds = 2
    now = int(time.time())
    expired_ts = now - (ttl_seconds + 1)
    active_ts = now
    expired_member = f"member:{expired_ts}"
    active_member = f"member:{active_ts}"

    # Add ZSET entries and corresponding hashes
    redis_client.zadd(zset_key, {expired_member: expired_ts, active_member: active_ts})
    redis_client.hset(expired_member, mapping={"v": "old"})
    redis_client.hset(active_member, mapping={"v": "new"})
    store._run_ttl_cleanup(redis_client, zset_key, ttl_seconds)
    # Check expired member removed, active remains
    remaining = redis_client.zrange(zset_key, 0, -1)
    assert active_member in remaining
    assert expired_member not in remaining
    assert not redis_client.exists(expired_member)
    redis_client.zadd(zset_key, {active_member: expired_ts})
    store._run_ttl_cleanup(redis_client, zset_key, ttl_seconds)
    assert not redis_client.exists(zset_key), "ZSET should be deleted when empty"


def test_ttl_cleanup_no_expired_members(repo_config):
    """Ensure TTL cleanup is a no-op when there are no expired members."""
    redis_client = _make_redis_client(repo_config)
    store = RedisOnlineStore()
    zset_key = "test:ttl_cleanup:no_expired"
    ttl_seconds = 5
    now = int(time.time())
    active_member = f"member:{now}"
    redis_client.zadd(zset_key, {active_member: now})
    redis_client.hset(active_member, mapping={"v": "new"})
    store._run_ttl_cleanup(redis_client, zset_key, ttl_seconds)
    remaining = redis_client.zrange(zset_key, 0, -1)
    assert active_member in remaining
    assert redis_client.exists(active_member)


def test_ttl_cleanup_empty_zset(repo_config):
    """Ensure cleanup safely returns when ZSET has no members."""
    redis_client = _make_redis_client(repo_config)
    store = RedisOnlineStore()
    zset_key = "test:ttl_cleanup:empty"

    # Create a ZSET and then remove all members to simulate an empty one
    redis_client.zadd(zset_key, {"temp": 1})
    redis_client.zrem(zset_key, "temp")
    assert redis_client.zcard(zset_key) == 0, "ZSET should be empty before cleanup"

    # Run cleanup
    store._run_ttl_cleanup(redis_client, zset_key, 10)
    assert not redis_client.exists(zset_key), "Empty ZSET should be deleted after cleanup"

def test_zset_trim_removes_old_members_and_deletes_empty_index(repo_config):
    """Ensure ZSET size cleanup trims correctly and removes empty indexes."""
    redis_client = _make_redis_client(repo_config)
    store = RedisOnlineStore()
    zset_key = "test:zset_trim:zset"
    max_events = 2
    members = {"m1": 1, "m2": 2, "m3": 3}
    redis_client.zadd(zset_key, members)
    for m in members:
        redis_client.hset(m, mapping={"v": m})
    # Trim to retain only latest max_events
    store._run_zset_trim(redis_client, zset_key, max_events)
    remaining = redis_client.zrange(zset_key, 0, -1)
    assert remaining == ["m2", "m3"]
    assert not redis_client.exists("m1"), "Oldest member's hash should be deleted"
    redis_client.zremrangebyrank(zset_key, 0, -1)
    store._run_zset_trim(redis_client, zset_key, max_events)
    assert not redis_client.exists(zset_key), "Empty ZSET index should be deleted"


def test_zset_trim_no_trim_needed(repo_config):
    """Ensure no-op when ZSET size <= max_events."""
    redis_client = _make_redis_client(repo_config)
    store = RedisOnlineStore()
    zset_key = "test:zset_trim:no_trim"
    max_events = 3
    members = {"a": 1, "b": 2, "c": 3}
    redis_client.zadd(zset_key, members)
    store._run_zset_trim(redis_client, zset_key, max_events)
    remaining = redis_client.zrange(zset_key, 0, -1)
    assert remaining == ["a", "b", "c"]


def test_zset_trim_no_popped_members(repo_config):
    """Ensure function handles case where zpopmin returns empty list."""
    redis_client = _make_redis_client(repo_config)
    store = RedisOnlineStore()
    zset_key = "test:zset_trim:no_popped"
    redis_client.zadd(zset_key, {"k1": 1, "k2": 2})
    original_zpopmin = redis_client.zpopmin
    redis_client.zpopmin = lambda *a, **kw: []
    store._run_zset_trim(redis_client, zset_key, 1)
    assert redis_client.exists(zset_key)
    redis_client.zpopmin = original_zpopmin


def test_zset_trim_delete_all_members(repo_config):
    """Ensure trimming can remove all members and delete empty ZSET."""
    redis_client = _make_redis_client(repo_config)
    store = RedisOnlineStore()
    zset_key = "test:zset_trim:delete_all"
    members = {"x1": 1, "x2": 2}
    redis_client.zadd(zset_key, members)
    for m in members:
        redis_client.hset(m, mapping={"v": m})
    store._run_zset_trim(redis_client, zset_key, 0)
    assert not redis_client.exists(zset_key)
