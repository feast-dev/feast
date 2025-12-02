import time
from datetime import datetime, timedelta

import pytest
from valkey import Valkey

from feast import Entity, Field, FileSource, RepoConfig, ValueType
from feast.infra.online_stores.eg_valkey import (
    EGValkeyOnlineStore,
    EGValkeyOnlineStoreConfig,
)
from feast.infra.online_stores.helpers import _mmh3, _redis_key
from feast.protos.feast.core.SortedFeatureView_pb2 import SortOrder
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.sorted_feature_view import SortedFeatureView, SortKey
from feast.types import (
    Float32,
    Int32,
    String,
    UnixTimestamp,
)
from tests.unit.infra.online_store.valkey_online_store_creator import (
    ValkeyOnlineStoreCreator,
)


@pytest.fixture
def valkey_online_store() -> EGValkeyOnlineStore:
    return EGValkeyOnlineStore()


@pytest.fixture(scope="session")
def valkey_online_store_config():
    creator = ValkeyOnlineStoreCreator("valkey_project")
    config = creator.create_online_store()
    yield config
    creator.teardown()


@pytest.fixture
def base_repo_config_kwargs():
    return dict(
        provider="local",
        project="test",
        entity_key_serialization_version=3,
        registry="dummy_registry.db",
    )


@pytest.fixture
def repo_config_without_docker_connection_string(base_repo_config_kwargs) -> RepoConfig:
    return RepoConfig(
        **base_repo_config_kwargs,
        online_store=EGValkeyOnlineStoreConfig(
            connection_string="valkey://localhost:6379",
        ),
    )


@pytest.fixture
def repo_config(valkey_online_store_config, base_repo_config_kwargs) -> RepoConfig:
    return RepoConfig(
        **base_repo_config_kwargs,
        online_store=EGValkeyOnlineStoreConfig(
            connection_string=valkey_online_store_config["connection_string"],
        ),
    )


@pytest.mark.docker
def test_valkey_online_write_batch_with_timestamp_as_sortkey(
    repo_config: RepoConfig,
    valkey_online_store: EGValkeyOnlineStore,
):
    (
        feature_view,
        data,
    ) = _create_sorted_feature_view_with_timestamp_as_sortkey()

    valkey_online_store.online_write_batch(
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

    r = Valkey(**conn_dict)

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

    zset_key_driver_1 = valkey_online_store.zset_key_bytes(
        feature_view.name, redis_key_bin_driver_1
    )

    entity_key_driver_2 = EntityKeyProto(
        join_keys=["driver_id"],
        entity_values=[ValueProto(int32_val=2)],
    )
    redis_key_bin_driver_2 = _redis_key(
        repo_config.project,
        entity_key_driver_2,
        entity_key_serialization_version=repo_config.entity_key_serialization_version,
    )

    zset_key_driver_2 = valkey_online_store.zset_key_bytes(
        feature_view.name, redis_key_bin_driver_2
    )

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
        hash_key = valkey_online_store.hash_key_bytes(redis_key_bin_driver_1, id)
        pipe.hgetall(hash_key)

    # Look up features for last 3 trips for driver 2
    for id in last_3_trips_driver_2:
        hash_key = valkey_online_store.hash_key_bytes(redis_key_bin_driver_2, id)
        pipe.hgetall(hash_key)

    features_list = pipe.execute()

    trip_id_feature_name = _mmh3(f"{feature_view.name}:trip_id")
    trip_id_drivers = []
    for feature_dict in features_list:
        val = ValueProto()
        val.ParseFromString(feature_dict[trip_id_feature_name])
        trip_id_drivers.append(val.int32_val)
    assert trip_id_drivers == [4, 3, 2, 9, 8, 7]


def test_multiple_sort_keys_not_supported(
    repo_config_without_docker_connection_string: RepoConfig,
    valkey_online_store: EGValkeyOnlineStore,
):
    (
        feature_view,
        data,
    ) = _create_sorted_feature_view_with_multiple_sortkeys()

    with pytest.raises(
        ValueError,
        match=r"Only one sort key is supported for Range query use cases in Valkey, but found 2 sort keys in the",
    ):
        valkey_online_store.online_write_batch(
            config=repo_config_without_docker_connection_string,
            table=feature_view,
            data=data,
            progress=None,
        )


def test_non_numeric_sort_key_not_supported(
    repo_config_without_docker_connection_string: RepoConfig,
    valkey_online_store: EGValkeyOnlineStore,
):
    (
        feature_view,
        data,
    ) = _create_sorted_feature_view_with_non_numeric_sortkey()

    with pytest.raises(
        TypeError, match=r"Unsupported sort key type STRING. Only timestamp"
    ):
        valkey_online_store.online_write_batch(
            config=repo_config_without_docker_connection_string,
            table=feature_view,
            data=data,
            progress=None,
        )


def _create_sorted_feature_view_with_timestamp_as_sortkey():
    fv = SortedFeatureView(
        name="driver_stats",
        source=FileSource(
            name="my_file_source",
            path="test.parquet",
            timestamp_field="event_timestamp",
        ),
        entities=[Entity(name="driver_id")],
        ttl=timedelta(minutes=100),
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


def _create_sorted_feature_view_with_multiple_sortkeys(n=10):
    fv = SortedFeatureView(
        name="driver_stats",
        source=FileSource(
            name="my_file_source",
            path="test.parquet",
            timestamp_field="event_timestamp",
        ),
        entities=[Entity(name="driver_id")],
        ttl=timedelta(seconds=100),
        sort_keys=[
            SortKey(
                name="rating",
                value_type=ValueType.FLOAT,
                default_sort_order=SortOrder.DESC,
            ),
            SortKey(
                name="trip_id",
                value_type=ValueType.INT32,
                default_sort_order=SortOrder.DESC,
            ),
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


def _create_sorted_feature_view_with_non_numeric_sortkey(n=10):
    fv = SortedFeatureView(
        name="driver_stats",
        source=FileSource(
            name="my_file_source",
            path="test.parquet",
            timestamp_field="event_timestamp",
        ),
        entities=[Entity(name="driver_id")],
        ttl=timedelta(seconds=100),
        sort_keys=[
            SortKey(
                name="rating",
                value_type=ValueType.STRING,
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
                dtype=String,
            ),
        ],
    )
    return fv, _make_rows()


def  _make_rows(n=10):
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
    return Valkey(host=host, port=int(port), decode_responses=False)


@pytest.mark.docker
def test_ttl_cleanup_removes_expired_members_and_index(repo_config):
    # Ensure TTL cleanup removes expired members, hashes, and deletes empty ZSETs.
    redis_client = _make_redis_client(repo_config)
    store = EGValkeyOnlineStore()
    zset_key = b"test:ttl_cleanup:zset"
    ttl_seconds = 2
    now_ms = int(time.time() * 1000)
    expired_ts_ms = now_ms - (ttl_seconds + 1) * 1000
    active_ts_ms = now_ms
    expired_member = f"member:{expired_ts_ms}".encode()
    active_member = f"member:{active_ts_ms}".encode()

    redis_client.zadd(
        zset_key, {expired_member: expired_ts_ms, active_member: active_ts_ms}
    )

    cutoff = (int(time.time()) - ttl_seconds) * 1000

    with redis_client.pipeline(transaction=False) as pipe:
        store._run_cleanup_by_event_time(pipe, zset_key, ttl_seconds, cutoff)
        pipe.execute()

    remaining = redis_client.zrange(zset_key, 0, -1)
    assert active_member in remaining
    assert expired_member not in remaining

    time.sleep(3)
    assert not redis_client.exists(zset_key), "ZSET should be deleted when empty"


@pytest.mark.docker
def test_ttl_cleanup_no_expired_members(repo_config):
    # Ensure TTL cleanup is a no-op when there are no expired members.
    redis_client = _make_redis_client(repo_config)
    store = EGValkeyOnlineStore()
    zset_key = b"test:ttl_cleanup:zset"
    ttl_seconds = 5
    now_ms = int(time.time() * 1000)
    active_ts_ms = now_ms
    active_member = f"member:{active_ts_ms}".encode()

    redis_client.zadd(zset_key, {active_member: active_ts_ms})

    cutoff = (int(time.time()) - ttl_seconds) * 1000

    with redis_client.pipeline(transaction=False) as pipe:
        store._run_cleanup_by_event_time(pipe, zset_key, ttl_seconds, cutoff)
        pipe.execute()

    remaining = redis_client.zrange(zset_key, 0, -1)
    assert active_member in remaining
