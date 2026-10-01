"""Integration tests comparing the redis-py and GLIDE Redis online read paths.

Rows are always written through the default redis-py client, then read back with
``client: redis`` and ``client: glide``. If GLIDE built different entity keys or
different hashed feature field names, the GLIDE reads would come back NOT_FOUND
instead of matching.

Run with: pytest --integration sdk/python/tests/integration/online_store/test_redis_glide_client.py
"""

import shutil
from datetime import datetime, timezone
from typing import Dict, List, Tuple

import pytest

from feast import Entity, FeatureView, Field, RepoConfig
from feast.infra.online_stores.redis import (
    RedisClient,
    RedisOnlineStore,
    RedisOnlineStoreConfig,
)
from feast.protos.feast.serving.ServingService_pb2 import GetOnlineFeaturesResponse
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RegistryConfig
from feast.types import Float32, Int64
from feast.value_type import ValueType

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def redis_address():
    if not shutil.which("docker"):
        pytest.skip("Docker not available")
    pytest.importorskip("glide_sync")

    from testcontainers.community.redis import RedisContainer

    container = RedisContainer("redis:7").with_exposed_ports(6379)
    container.start()
    try:
        yield f"{container.get_container_host_ip()}:{container.get_exposed_port(6379)}"
    finally:
        container.stop()


def _feature_views() -> Tuple[FeatureView, FeatureView]:
    entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    driver_stats = FeatureView(
        name="driver_stats",
        entities=[entity],
        schema=[
            Field(name="trips_today", dtype=Int64),
            Field(name="avg_rating", dtype=Float32),
        ],
    )
    driver_metrics = FeatureView(
        name="driver_metrics",
        entities=[entity],
        schema=[Field(name="lifetime_trips", dtype=Int64)],
    )
    return driver_stats, driver_metrics


def _entity_key(driver_id: int) -> EntityKeyProto:
    return EntityKeyProto(
        join_keys=["driver_id"], entity_values=[ValueProto(int64_val=driver_id)]
    )


def _config(redis_address: str, client: RedisClient) -> RepoConfig:
    return RepoConfig(
        project="test_redis_glide",
        provider="local",
        online_store=RedisOnlineStoreConfig(
            connection_string=redis_address, client=client
        ),
        registry=RegistryConfig(path="/tmp/test_redis_glide_registry.pb"),
        entity_key_serialization_version=3,
    )


def _seed(redis_address: str, driver_ids: List[int]) -> None:
    """Write rows with the default redis-py client so both paths read the same data."""
    driver_stats, driver_metrics = _feature_views()
    config = _config(redis_address, RedisClient.redis)
    store = RedisOnlineStore()
    now = datetime.now(tz=timezone.utc)

    store.online_write_batch(
        config,
        driver_stats,
        [
            (
                _entity_key(driver_id),
                {
                    "trips_today": ValueProto(int64_val=driver_id * 10),
                    "avg_rating": ValueProto(float_val=driver_id / 2),
                },
                now,
                now,
            )
            for driver_id in driver_ids
        ],
        progress=None,
    )
    store.online_write_batch(
        config,
        driver_metrics,
        [
            (
                _entity_key(driver_id),
                {"lifetime_trips": ValueProto(int64_val=driver_id * 100)},
                now,
                now,
            )
            for driver_id in driver_ids
        ],
        progress=None,
    )


def _batched_response(
    redis_address: str,
    client: RedisClient,
    entity_ids: List[int],
) -> GetOnlineFeaturesResponse:
    """Drive the batched per-feature-view read that backs get_online_features."""
    driver_stats, driver_metrics = _feature_views()
    response = GetOnlineFeaturesResponse()
    RedisOnlineStore()._read_features_per_fv(
        config=_config(redis_address, client),
        grouped_refs=[
            (driver_stats, ["trips_today", "avg_rating"]),
            (driver_metrics, ["lifetime_trips"]),
        ],
        join_key_values={
            "driver_id": [ValueProto(int64_val=driver_id) for driver_id in entity_ids]
        },
        entity_name_to_join_key_map={"driver_id": "driver_id"},
        online_features_response=response,
        full_feature_names=False,
        include_feature_view_version_metadata=False,
    )
    return response


def _features(row) -> Dict[str, ValueProto]:
    """Unwrap an online_read row, asserting the entity was found."""
    features = row[1]
    assert features is not None, "expected a value for this entity"
    return features


def _values_by_feature(response: GetOnlineFeaturesResponse) -> Dict[str, List[float]]:
    """Per requested feature name, the value of each output row (0.0 when absent).

    ``response.results`` holds one entry per feature, and each entry's ``values``
    holds one value per output row.
    """
    return {
        name: [
            value.int64_val or value.float_val
            for value in response.results[index].values
        ]
        for index, name in enumerate(response.metadata.feature_names.val)
    }


def test_glide_online_read_matches_redis_py(redis_address):
    driver_stats, _ = _feature_views()
    seeded = [1001, 1002, 1003]
    _seed(redis_address, seeded)

    # 9999 was never written, so both paths must agree on the empty row.
    entity_keys = [_entity_key(driver_id) for driver_id in seeded + [9999]]
    requested = ["trips_today", "avg_rating"]

    redis_rows = RedisOnlineStore().online_read(
        _config(redis_address, RedisClient.redis),
        driver_stats,
        entity_keys,
        list(requested),
    )
    glide_rows = RedisOnlineStore().online_read(
        _config(redis_address, RedisClient.glide),
        driver_stats,
        entity_keys,
        list(requested),
    )

    assert glide_rows == redis_rows
    # Guard against both paths agreeing on nothing.
    assert _features(glide_rows[0])["trips_today"].int64_val == 10010
    assert _features(glide_rows[2])["trips_today"].int64_val == 10030
    assert _features(glide_rows[3])["trips_today"].WhichOneof("val") is None


def test_glide_batched_read_matches_redis_py(redis_address):
    seeded = [2001, 2002, 2003]
    _seed(redis_address, seeded)

    # 2004 was never written, so both paths must agree on the NOT_FOUND row.
    entity_ids = [2001, 2002, 2004, 2003]

    redis_response = _batched_response(redis_address, RedisClient.redis, entity_ids)
    glide_response = _batched_response(redis_address, RedisClient.glide, entity_ids)

    assert glide_response == redis_response
    assert list(glide_response.metadata.feature_names.val) == list(
        redis_response.metadata.feature_names.val
    )

    values = _values_by_feature(glide_response)
    assert values["trips_today"] == [20010, 20020, 0, 20030]
    assert values["avg_rating"] == [1000.5, 1001.0, 0, 1001.5]
    assert values["lifetime_trips"] == [200100, 200200, 0, 200300]
