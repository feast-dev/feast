"""Integration tests for DynamoDB online store feature view versioning.

Run with: pytest --integration sdk/python/tests/integration/online_store/test_dynamodb_versioning.py

Uses moto to mock the DynamoDB service (no Docker required).
"""

import os
from datetime import datetime, timedelta, timezone

import pytest

from feast import Entity, FeatureView
from feast.field import Field
from feast.infra.online_stores.dynamodb import DynamoDBOnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RegistryConfig, RepoConfig
from feast.types import Float32, Int64
from feast.value_type import ValueType


def _make_feature_view(name="driver_stats", version="latest"):
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
        version=version,
    )


def _make_entity_key(driver_id: int) -> EntityKeyProto:
    entity_key = EntityKeyProto()
    entity_key.join_keys.append("driver_id")
    val = ValueProto()
    val.int64_val = driver_id
    entity_key.entity_values.append(val)
    return entity_key


def _write_and_read(store, config, fv, driver_id=1001, trips=42):
    entity_key = _make_entity_key(driver_id)
    val = ValueProto()
    val.int64_val = trips
    now = datetime.now(tz=timezone.utc)
    store.online_write_batch(
        config, fv, [(entity_key, {"trips_today": val}, now, now)], None
    )
    return store.online_read(config, fv, [entity_key], ["trips_today"])


def _make_config(enable_versioning=False):
    from feast.infra.online_stores.dynamodb import DynamoDBOnlineStoreConfig

    return RepoConfig(
        project="test_project",
        provider="local",
        online_store=DynamoDBOnlineStoreConfig(
            type="dynamodb",
            region="us-east-1",
        ),
        registry=RegistryConfig(
            path="/tmp/test_dynamodb_registry.pb",
            enable_online_feature_view_versioning=enable_versioning,
        ),
        entity_key_serialization_version=3,
    )


@pytest.mark.integration
class TestDynamoDBVersioningIntegration:
    """Integration tests for DynamoDB versioning using moto mock."""

    @pytest.fixture(autouse=True)
    def setup_dynamodb(self):
        try:
            from moto import mock_dynamodb
        except ImportError:
            pytest.skip("moto not installed")

        # Set dummy AWS credentials for moto
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # noqa: S105
        os.environ["AWS_SECURITY_TOKEN"] = "testing"  # noqa: S105
        os.environ["AWS_SESSION_TOKEN"] = "testing"  # noqa: S105
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        with mock_dynamodb():
            yield

    def test_write_read_without_versioning(self):
        config = _make_config(enable_versioning=False)
        store = DynamoDBOnlineStore()
        fv = _make_feature_view()
        store.update(config, [], [fv], [], [], False)

        result = _write_and_read(store, config, fv)
        assert result[0][1] is not None
        assert result[0][1]["trips_today"].int64_val == 42

    def test_write_read_with_versioning_v1(self):
        config = _make_config(enable_versioning=True)
        store = DynamoDBOnlineStore()
        fv = _make_feature_view()
        fv.current_version_number = 1
        store.update(config, [], [fv], [], [], False)

        result = _write_and_read(store, config, fv)
        assert result[0][1] is not None
        assert result[0][1]["trips_today"].int64_val == 42

    def test_version_isolation(self):
        """Data written to v1 is not visible from v2."""
        config = _make_config(enable_versioning=True)
        store = DynamoDBOnlineStore()

        fv_v1 = _make_feature_view()
        fv_v1.current_version_number = 1
        store.update(config, [], [fv_v1], [], [], False)
        _write_and_read(store, config, fv_v1, driver_id=1001, trips=10)

        fv_v2 = _make_feature_view()
        fv_v2.current_version_number = 2
        store.update(config, [], [fv_v2], [], [], False)

        entity_key = _make_entity_key(1001)
        result = store.online_read(config, fv_v2, [entity_key], ["trips_today"])
        assert result[0] == (None, None)

        result = store.online_read(config, fv_v1, [entity_key], ["trips_today"])
        assert result[0][1] is not None
        assert result[0][1]["trips_today"].int64_val == 10

    def test_projection_version_tag_routes_to_correct_table(self):
        """projection.version_tag routes reads to the correct versioned DynamoDB table."""
        config = _make_config(enable_versioning=True)
        store = DynamoDBOnlineStore()

        fv_v1 = _make_feature_view()
        fv_v1.current_version_number = 1
        store.update(config, [], [fv_v1], [], [], False)
        _write_and_read(store, config, fv_v1, driver_id=1001, trips=100)

        fv_v2 = _make_feature_view()
        fv_v2.current_version_number = 2
        store.update(config, [], [fv_v2], [], [], False)
        _write_and_read(store, config, fv_v2, driver_id=1001, trips=200)

        fv_read = _make_feature_view()
        fv_read.projection.version_tag = 1
        entity_key = _make_entity_key(1001)
        result = store.online_read(config, fv_read, [entity_key], ["trips_today"])
        assert result[0][1]["trips_today"].int64_val == 100

        fv_read2 = _make_feature_view()
        fv_read2.projection.version_tag = 2
        result = store.online_read(config, fv_read2, [entity_key], ["trips_today"])
        assert result[0][1]["trips_today"].int64_val == 200

    def test_teardown_versioned_table(self):
        """teardown() drops the versioned DynamoDB table without error."""
        config = _make_config(enable_versioning=True)
        store = DynamoDBOnlineStore()

        fv = _make_feature_view()
        fv.current_version_number = 1
        store.update(config, [], [fv], [], [], False)
        _write_and_read(store, config, fv)

        # Should not raise
        store.teardown(config, [fv], [])

    def test_update_deletes_versioned_table(self):
        """update() with tables_to_delete correctly drops versioned DynamoDB tables."""
        config = _make_config(enable_versioning=True)
        store = DynamoDBOnlineStore()

        fv = _make_feature_view()
        fv.current_version_number = 1
        store.update(config, [], [fv], [], [], False)
        _write_and_read(store, config, fv, driver_id=1001, trips=50)

        # Delete the versioned table
        store.update(config, [fv], [], [], [], False)
