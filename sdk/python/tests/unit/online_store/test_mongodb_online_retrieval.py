"""
Unit tests for MongoDB online store.

Docker-dependent tests are marked with ``@_requires_docker`` and are skipped when
Docker is unavailable.  Pure Python tests (no container needed) run in all environments.
"""

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("pymongo")

from feast import FeatureView, Field, FileSource  # noqa: E402
from feast.infra.online_stores.mongodb_online_store.mongodb import (  # noqa: E402
    MongoDBOnlineStore,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.types import Int64
from feast.utils import _utc_now
from tests.integration.feature_repos.universal.feature_views import TAGS
from tests.utils.cli_repo_creator import CliRunner, get_example_repo

# Check if Docker is available
docker_available = False
try:
    import docker
    from testcontainers.mongodb import MongoDbContainer

    # Try to connect to Docker daemon
    try:
        client = docker.from_env()
        client.ping()
        docker_available = True
    except Exception:
        pass
except ImportError:
    pass

# Applied per-test so that pure Python tests still run without Docker.
_requires_docker = pytest.mark.skipif(
    not docker_available,
    reason="Docker is not available or not running. Start Docker daemon to run these tests.",
)


@pytest.fixture(scope="module")
def mongodb_container():
    """Start a MongoDB container for testing."""
    container = MongoDbContainer(
        "mongo:latest",
        username="test",
        password="test",  # pragma: allowlist secret
    ).with_exposed_ports(27017)
    container.start()
    yield container
    container.stop()


@pytest.fixture
def mongodb_connection_string(mongodb_container):
    """Get MongoDB connection string from the container."""
    exposed_port = mongodb_container.get_exposed_port(27017)
    return f"mongodb://test:test@localhost:{exposed_port}"  # pragma: allowlist secret


@_requires_docker
def test_mongodb_online_features(mongodb_connection_string):
    """
    Test reading from MongoDB online store using testcontainers.
    """
    runner = CliRunner()
    with (
        runner.local_repo(
            get_example_repo("example_feature_repo_1.py"),
            offline_store="file",
            online_store="mongodb",
            teardown=False,  # Disable CLI teardown since container will be stopped by fixture
        ) as store
    ):
        # Update the connection string to use the test container
        store.config.online_store.connection_string = mongodb_connection_string

        # Write some data to two tables
        driver_locations_fv = store.get_feature_view(name="driver_locations")
        customer_profile_fv = store.get_feature_view(name="customer_profile")
        customer_driver_combined_fv = store.get_feature_view(
            name="customer_driver_combined"
        )

        provider = store._get_provider()

        driver_key = EntityKeyProto(
            join_keys=["driver_id"], entity_values=[ValueProto(int64_val=1)]
        )
        provider.online_write_batch(
            config=store.config,
            table=driver_locations_fv,
            data=[
                (
                    driver_key,
                    {
                        "lat": ValueProto(double_val=0.1),
                        "lon": ValueProto(string_val="1.0"),
                    },
                    _utc_now(),
                    _utc_now(),
                )
            ],
            progress=None,
        )

        customer_key = EntityKeyProto(
            join_keys=["customer_id"], entity_values=[ValueProto(string_val="5")]
        )
        provider.online_write_batch(
            config=store.config,
            table=customer_profile_fv,
            data=[
                (
                    customer_key,
                    {
                        "avg_orders_day": ValueProto(float_val=1.0),
                        "name": ValueProto(string_val="John"),
                        "age": ValueProto(int64_val=3),
                    },
                    _utc_now(),
                    _utc_now(),
                )
            ],
            progress=None,
        )

        customer_key = EntityKeyProto(
            join_keys=["customer_id", "driver_id"],
            entity_values=[ValueProto(string_val="5"), ValueProto(int64_val=1)],
        )
        provider.online_write_batch(
            config=store.config,
            table=customer_driver_combined_fv,
            data=[
                (
                    customer_key,
                    {"trips": ValueProto(int64_val=7)},
                    _utc_now(),
                    _utc_now(),
                )
            ],
            progress=None,
        )

        assert len(store.list_entities()) == 3
        assert len(store.list_entities(tags=TAGS)) == 2

        # Retrieve features using two keys
        result = store.get_online_features(
            features=[
                "driver_locations:lon",
                "customer_profile:avg_orders_day",
                "customer_profile:name",
                "customer_driver_combined:trips",
            ],
            entity_rows=[
                {"driver_id": 1, "customer_id": "5"},
                {"driver_id": 1, "customer_id": 5},
            ],
            full_feature_names=False,
        ).to_dict()

        assert "lon" in result
        assert "avg_orders_day" in result
        assert "name" in result
        assert result["driver_id"] == [1, 1]
        assert result["customer_id"] == ["5", "5"]
        assert result["lon"] == ["1.0", "1.0"]
        assert result["avg_orders_day"] == [1.0, 1.0]
        assert result["name"] == ["John", "John"]
        assert result["trips"] == [7, 7]

        # Ensure features are still in result when keys not found
        result = store.get_online_features(
            features=["customer_driver_combined:trips"],
            entity_rows=[{"driver_id": 0, "customer_id": 0}],
            full_feature_names=False,
        ).to_dict()

        assert result["trips"] == [None]


# ---------------------------------------------------------------------------
# Pure Python tests — no Docker required
# ---------------------------------------------------------------------------


def _make_fv(*field_names: str) -> FeatureView:
    """Build a minimal FeatureView with Int64 features for use in unit tests."""
    return FeatureView(
        name="test_fv",
        entities=[],
        schema=[Field(name=n, dtype=Int64) for n in field_names],
        source=FileSource(path="fake.parquet", timestamp_field="event_timestamp"),
        ttl=timedelta(days=1),
    )


def test_convert_raw_docs_missing_entity():
    """Entity key absent from docs → result tuple is (None, None) for that position."""
    fv = _make_fv("score")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ids = [b"present", b"missing"]
    docs = {
        b"present": {
            "features": {"test_fv": {"score": 42}},
            "event_timestamps": {"test_fv": ts},
        }
    }

    results = MongoDBOnlineStore._convert_raw_docs_to_proto(ids, docs, fv)

    assert len(results) == 2
    ts_out, feats_out = results[0]
    assert ts_out == ts
    assert feats_out["score"].int64_val == 42
    assert results[1] == (None, None)


def test_convert_raw_docs_partial_doc():
    """Entity exists but one feature key is absent → empty ValueProto for that feature."""
    fv = _make_fv("present_feat", "missing_feat")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ids = [b"entity1"]
    docs = {
        b"entity1": {
            # missing_feat intentionally omitted (e.g. schema migration scenario)
            "features": {"test_fv": {"present_feat": 99}},
            "event_timestamps": {"test_fv": ts},
        }
    }

    results = MongoDBOnlineStore._convert_raw_docs_to_proto(ids, docs, fv)

    assert len(results) == 1
    ts_out, feats_out = results[0]
    assert ts_out == ts
    assert feats_out["present_feat"].int64_val == 99
    assert feats_out["missing_feat"] == ValueProto()  # null / not-set


def test_convert_raw_docs_entity_exists_but_fv_not_written():
    """Entity doc exists (written by another FV) but this FV was never written → (None, None).

    MongoDB stores all feature views for the same entity in one document.
    If FV "driver_stats" was written, an entity doc exists for driver_1.
    A subsequent read for FV "pricing" (never written) must return (None, None),
    not a truthy dict of empty ValueProtos.
    """
    pricing_fv = _make_fv("price")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ids = [b"driver_1"]
    # doc was created by driver_stats, pricing key is absent entirely
    docs = {
        b"driver_1": {
            "features": {"driver_stats": {"acc_rate": 0.9}},
            "event_timestamps": {"driver_stats": ts},
        }
    }

    results = MongoDBOnlineStore._convert_raw_docs_to_proto(ids, docs, pricing_fv)

    assert len(results) == 1
    assert results[0] == (None, None)


def test_convert_raw_docs_ordering():
    """Result order matches the ids list regardless of dict insertion order in docs."""
    fv = _make_fv("score")
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)

    # Request entity keys in z → a → m order
    ids = [b"entity_z", b"entity_a", b"entity_m"]

    # docs is in a different order (simulating arbitrary MongoDB cursor return order)
    docs = {
        b"entity_a": {
            "features": {"test_fv": {"score": 2}},
            "event_timestamps": {"test_fv": ts},
        },
        b"entity_m": {
            "features": {"test_fv": {"score": 3}},
            "event_timestamps": {"test_fv": ts},
        },
        b"entity_z": {
            "features": {"test_fv": {"score": 1}},
            "event_timestamps": {"test_fv": ts},
        },
    }

    results = MongoDBOnlineStore._convert_raw_docs_to_proto(ids, docs, fv)

    assert len(results) == 3
    # Results must follow the ids order: z=1, a=2, m=3
    assert results[0][1]["score"].int64_val == 1  # entity_z
    assert results[1][1]["score"].int64_val == 2  # entity_a
    assert results[2][1]["score"].int64_val == 3  # entity_m
