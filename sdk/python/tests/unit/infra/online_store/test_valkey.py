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
def repo_config(valkey_online_store_config):
    return RepoConfig(
        provider="local",
        project="test",
        online_store=EGValkeyOnlineStoreConfig(
            connection_string=valkey_online_store_config["connection_string"],
        ),
        entity_key_serialization_version=3,
        registry="dummy_registry.db",
    )


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


def test_valkey_online_write_batch_with_float_as_sortkey(
    repo_config: RepoConfig,
    valkey_online_store: EGValkeyOnlineStore,
):
    (
        feature_view,
        data,
    ) = _create_sorted_feature_view_with_float_as_sortkey()

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

    # Get trips for driver 1 where ratings between 2.5 and 4.5
    # Get trips for driver 2 where ratings between 7.5 and 9.5
    driver_1_trips = r.zrangebyscore(zset_key_driver_1, 2.5, 4.5)
    driver_2_trips = r.zrangebyscore(zset_key_driver_2, 7.5, 9.5)

    # Look up features for trips for driver 1
    for id in driver_1_trips:
        hash_key = valkey_online_store.hash_key_bytes(redis_key_bin_driver_1, id)
        pipe.hgetall(hash_key)

    # Look up features for trips for driver 2
    for id in driver_2_trips:
        hash_key = valkey_online_store.hash_key_bytes(redis_key_bin_driver_2, id)
        pipe.hgetall(hash_key)

    features_list = pipe.execute()

    trip_id_feature_name = _mmh3(f"{feature_view.name}:trip_id")
    trip_id_drivers = []
    for feature_dict in features_list:
        val = ValueProto()
        val.ParseFromString(feature_dict[trip_id_feature_name])
        trip_id_drivers.append(val.int32_val)
    assert trip_id_drivers == [2, 3, 4, 7, 8, 9]


def test_multiple_sort_keys_not_supported(
    repo_config: RepoConfig, valkey_online_store: EGValkeyOnlineStore
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
            config=repo_config,
            table=feature_view,
            data=data,
            progress=None,
        )


def test_non_numeric_sort_key_not_supported(
    repo_config: RepoConfig, valkey_online_store: EGValkeyOnlineStore
):
    (
        feature_view,
        data,
    ) = _create_sorted_feature_view_with_non_numeric_sortkey()

    with pytest.raises(
        TypeError, match=r"Unsupported sort key type STRING. Only numerics or timestamp"
    ):
        valkey_online_store.online_write_batch(
            config=repo_config,
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


def _create_sorted_feature_view_with_multiple_sortkeys(n=10):
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
        ttl=timedelta(seconds=10),
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


def _create_sorted_feature_view_with_float_as_sortkey(n=10):
    fv = SortedFeatureView(
        name="driver_stats_float",
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
