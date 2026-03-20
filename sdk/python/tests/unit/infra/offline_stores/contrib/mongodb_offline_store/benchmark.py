"""
Performance benchmarks comparing the two MongoDB offline store implementations -
one Collection with all feature views
vs. a schema of N collections for N features views.

These tests measure performance across different scaling dimensions:
1. Row count scaling (entity_df size)
2. Feature width scaling (features per FeatureView)
3. Entity distribution (unique vs skewed/repeated entity_ids)

Metrics captured:
- Runtime (wall clock)
- Memory (peak Python memory via tracemalloc)
- MongoDB server metrics (opcounters, execution stats)

Run with: pytest benchmark_mongodb_offline_stores.py -v -s
Skip slow tests: pytest benchmark_mongodb_offline_stores.py -v -s -m "not slow"
"""

import time
import tracemalloc
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, Optional

import pandas as pd
import pytest
import pytz

pytest.importorskip("pymongo")

from unittest.mock import MagicMock

from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer

from feast import Entity, FeatureView, Field
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_many import (
    MongoDBOfflineStoreMany,
    MongoDBOfflineStoreManyConfig,
    MongoDBSourceMany,
)
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_one import (
    MongoDBOfflineStoreOne,
    MongoDBOfflineStoreOneConfig,
    MongoDBSourceOne,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import RepoConfig
from feast.types import Float64, Int64
from feast.value_type import ValueType

# Check if Docker is available
docker_available = False
try:
    import docker

    try:
        client = docker.from_env()
        client.ping()
        docker_available = True
    except Exception:
        pass
except ImportError:
    pass

_requires_docker = pytest.mark.skipif(
    not docker_available,
    reason="Docker is not available or not running.",
)

ENTITY_KEY_VERSION = 3


@dataclass
class BenchmarkResult:
    """Container for benchmark results."""

    implementation: str
    test_name: str
    dimension: str
    value: int
    duration_seconds: float
    rows_per_second: float
    peak_memory_mb: float = 0.0
    mongo_docs_examined: int = 0
    mongo_keys_examined: int = 0
    mongo_execution_time_ms: int = 0


@dataclass
class MongoMetrics:
    """MongoDB server metrics captured before/after a query."""

    opcounters: Dict[str, int] = field(default_factory=dict)
    docs_examined: int = 0
    keys_examined: int = 0

    @staticmethod
    def capture(client: Any) -> "MongoMetrics":
        """Capture current MongoDB server metrics."""
        status = client.admin.command("serverStatus")
        return MongoMetrics(
            opcounters=dict(status.get("opcounters", {})),
        )

    def delta(self, after: "MongoMetrics") -> Dict[str, int]:
        """Calculate delta between two metric snapshots."""
        return {
            k: after.opcounters.get(k, 0) - self.opcounters.get(k, 0)
            for k in after.opcounters
        }


def _make_entity_id(driver_id: int) -> bytes:
    """Create serialized entity key."""
    entity_key = EntityKeyProto()
    entity_key.join_keys.append("driver_id")
    val = ValueProto()
    val.int64_val = driver_id
    entity_key.entity_values.append(val)
    return serialize_entity_key(entity_key, ENTITY_KEY_VERSION)


@pytest.fixture(scope="module")
def mongodb_container() -> Generator[MongoDbContainer, None, None]:
    """Start a MongoDB container for benchmarks."""
    container = MongoDbContainer(
        "mongo:latest",
        username="test",
        password="test",  # pragma: allowlist secret
    ).with_exposed_ports(27017)
    container.start()
    yield container
    container.stop()


@pytest.fixture
def mongodb_connection_string(mongodb_container: MongoDbContainer) -> str:
    """Get MongoDB connection string."""
    exposed_port = mongodb_container.get_exposed_port(27017)
    return f"mongodb://test:test@localhost:{exposed_port}"  # pragma: allowlist secret


@pytest.fixture
def ibis_config(mongodb_connection_string: str) -> RepoConfig:
    """RepoConfig for Ibis implementation."""
    return RepoConfig(
        project="benchmark",
        registry="memory://",
        provider="local",
        offline_store=MongoDBOfflineStoreManyConfig(
            connection_string=mongodb_connection_string,
            database="benchmark_db",
        ),
        online_store={"type": "sqlite"},
        entity_key_serialization_version=ENTITY_KEY_VERSION,
    )


@pytest.fixture
def native_config(mongodb_connection_string: str) -> RepoConfig:
    """RepoConfig for Native implementation."""
    return RepoConfig(
        project="benchmark",
        registry="memory://",
        provider="local",
        offline_store=MongoDBOfflineStoreOneConfig(
            connection_string=mongodb_connection_string,
            database="benchmark_db",
            collection="feature_history",
        ),
        online_store={"type": "sqlite"},
        entity_key_serialization_version=ENTITY_KEY_VERSION,
    )


def _generate_ibis_data(
    client: MongoClient,
    db_name: str,
    collection_name: str,
    num_entities: int,
    num_features: int,
    rows_per_entity: int = 5,
) -> datetime:
    """Generate test data for Ibis (one collection per FV, flat schema)."""
    collection = client[db_name][collection_name]
    collection.drop()

    now = datetime.now(tz=pytz.UTC)
    docs = []

    for entity_id in range(num_entities):
        for row in range(rows_per_entity):
            doc = {
                "driver_id": entity_id,
                "event_timestamp": now - timedelta(hours=row),
            }
            for f in range(num_features):
                doc[f"feature_{f}"] = float(entity_id * 100 + f + row * 0.1)
            docs.append(doc)

    collection.insert_many(docs)
    return now


def _generate_native_data(
    client: MongoClient,
    db_name: str,
    collection_name: str,
    feature_view_name: str,
    num_entities: int,
    num_features: int,
    rows_per_entity: int = 5,
) -> datetime:
    """Generate test data for Native (single collection, nested features)."""
    collection = client[db_name][collection_name]
    # Don't drop - may have multiple FVs in same collection

    now = datetime.now(tz=pytz.UTC)
    docs = []

    for entity_id in range(num_entities):
        for row in range(rows_per_entity):
            features = {}
            for f in range(num_features):
                features[f"feature_{f}"] = float(entity_id * 100 + f + row * 0.1)

            doc = {
                "entity_id": _make_entity_id(entity_id),
                "feature_view": feature_view_name,
                "features": features,
                "event_timestamp": now - timedelta(hours=row),
                "created_at": now - timedelta(hours=row),
            }
            docs.append(doc)

    collection.insert_many(docs)
    return now


def _create_ibis_fv(num_features: int) -> tuple:
    """Create Ibis source and FeatureView."""
    source = MongoDBSourceMany(
        name="driver_benchmark",
        database="benchmark_db",
        collection="driver_benchmark",
        timestamp_field="event_timestamp",
    )
    entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )

    schema = [Field(name="driver_id", dtype=Int64)]
    for f in range(num_features):
        schema.append(Field(name=f"feature_{f}", dtype=Float64))

    fv = FeatureView(
        name="driver_benchmark",
        entities=[entity],
        schema=schema,
        source=source,
        ttl=timedelta(days=1),
    )
    return source, fv


def _create_native_fv(num_features: int) -> tuple:
    """Create Native source and FeatureView."""
    source = MongoDBSourceOne(
        name="driver_benchmark",
        timestamp_field="event_timestamp",
    )
    entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )

    schema = [Field(name="driver_id", dtype=Int64)]
    for f in range(num_features):
        schema.append(Field(name=f"feature_{f}", dtype=Float64))

    fv = FeatureView(
        name="driver_benchmark",
        entities=[entity],
        schema=schema,
        source=source,
        ttl=timedelta(days=1),
    )
    return source, fv


def _run_benchmark(func, name: str) -> float:
    """Run a function and return elapsed time."""
    start = time.perf_counter()
    func()  # Execute the function
    elapsed = time.perf_counter() - start
    return elapsed


@dataclass
class FullBenchmarkResult:
    """Full benchmark results with all metrics."""

    elapsed_seconds: float
    peak_memory_mb: float
    mongo_opcounters_delta: Dict[str, int]


def _run_benchmark_full(
    func,
    mongo_client: Optional[Any] = None,
) -> FullBenchmarkResult:
    """Run a benchmark capturing runtime, memory, and MongoDB metrics."""
    # Capture MongoDB metrics before
    mongo_before = None
    if mongo_client:
        mongo_before = MongoMetrics.capture(mongo_client)

    # Start memory tracking
    tracemalloc.start()

    # Run the benchmark
    start = time.perf_counter()
    func()
    elapsed = time.perf_counter() - start

    # Capture peak memory
    _, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    peak_memory_mb = peak_memory / (1024 * 1024)

    # Capture MongoDB metrics after
    mongo_delta = {}
    if mongo_client and mongo_before:
        mongo_after = MongoMetrics.capture(mongo_client)
        mongo_delta = mongo_before.delta(mongo_after)

    return FullBenchmarkResult(
        elapsed_seconds=elapsed,
        peak_memory_mb=peak_memory_mb,
        mongo_opcounters_delta=mongo_delta,
    )


def _print_benchmark_result(
    impl: str,
    dimension_name: str,
    dimension_value: int,
    result: FullBenchmarkResult,
    num_rows: Optional[int] = None,
) -> None:
    """Pretty print benchmark results."""
    print(f"\n[{impl}] {dimension_name}: {dimension_value:,}")
    print(f"  Time:   {result.elapsed_seconds:.3f}s")
    print(f"  Memory: {result.peak_memory_mb:.1f} MB")
    if num_rows:
        rate = num_rows / result.elapsed_seconds if result.elapsed_seconds > 0 else 0
        print(f"  Rate:   {rate:,.0f} rows/s")
    if result.mongo_opcounters_delta:
        print(f"  Mongo ops: {result.mongo_opcounters_delta}")


# =============================================================================
# Test 1: Scale Rows (entity_df size)
# =============================================================================

ROW_COUNTS = [
    1000,
    5000,
    10000,
]  # Reduced for CI; use [10000, 50000, 100000, 500000] for full benchmark


@_requires_docker
@pytest.mark.parametrize("num_rows", ROW_COUNTS)
def test_scale_rows_ibis(
    mongodb_connection_string: str, ibis_config: RepoConfig, num_rows: int
) -> None:
    """Benchmark Ibis implementation with varying entity_df sizes.

    Measures: runtime, peak memory, MongoDB opcounters.
    """
    num_features = 10
    num_entities = num_rows  # One row per entity for simplicity

    client = MongoClient(mongodb_connection_string)
    try:
        now = _generate_ibis_data(
            client,
            "benchmark_db",
            "driver_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=3,
        )

        _, fv = _create_ibis_fv(num_features)

        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        feature_refs = [f"driver_benchmark:feature_{i}" for i in range(num_features)]

        def run_query():
            job = MongoDBOfflineStoreMany.get_historical_features(
                config=ibis_config,
                feature_views=[fv],
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return job.to_df()

        result = _run_benchmark_full(run_query, mongo_client=client)
        _print_benchmark_result("IBIS", "Rows", num_rows, result, num_rows=num_rows)

    finally:
        client.close()


@_requires_docker
@pytest.mark.parametrize("num_rows", ROW_COUNTS)
def test_scale_rows_native(
    mongodb_connection_string: str, native_config: RepoConfig, num_rows: int
) -> None:
    """Benchmark Native implementation with varying entity_df sizes.

    Measures: runtime, peak memory, MongoDB opcounters.
    """
    num_features = 10
    num_entities = num_rows

    client = MongoClient(mongodb_connection_string)
    try:
        client["benchmark_db"]["feature_history"].drop()
        now = _generate_native_data(
            client,
            "benchmark_db",
            "feature_history",
            "driver_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=3,
        )

        _, fv = _create_native_fv(num_features)

        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        feature_refs = [f"driver_benchmark:feature_{i}" for i in range(num_features)]

        def run_query():
            job = MongoDBOfflineStoreOne.get_historical_features(
                config=native_config,
                feature_views=[fv],
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return job.to_df()

        result = _run_benchmark_full(run_query, mongo_client=client)
        _print_benchmark_result("NATIVE", "Rows", num_rows, result, num_rows=num_rows)

    finally:
        client.close()


# =============================================================================
# Test 2: Wide Feature Views (features per FV)
# =============================================================================

FEATURE_COUNTS = [10, 50, 100]  # Use [50, 100, 150, 200] for full benchmark


@_requires_docker
@pytest.mark.parametrize("num_features", FEATURE_COUNTS)
def test_wide_features_ibis(
    mongodb_connection_string: str, ibis_config: RepoConfig, num_features: int
) -> None:
    """Benchmark Ibis with varying feature width."""
    num_entities = 1000

    client = MongoClient(mongodb_connection_string)
    try:
        now = _generate_ibis_data(
            client,
            "benchmark_db",
            "driver_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=3,
        )

        _, fv = _create_ibis_fv(num_features)

        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        feature_refs = [f"driver_benchmark:feature_{i}" for i in range(num_features)]

        def run_query():
            job = MongoDBOfflineStoreMany.get_historical_features(
                config=ibis_config,
                feature_views=[fv],
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return job.to_df()

        result = _run_benchmark_full(run_query, mongo_client=client)
        _print_benchmark_result(
            "IBIS", "Features", num_features, result, num_rows=num_entities
        )

    finally:
        client.close()


@_requires_docker
@pytest.mark.parametrize("num_features", FEATURE_COUNTS)
def test_wide_features_native(
    mongodb_connection_string: str, native_config: RepoConfig, num_features: int
) -> None:
    """Benchmark Native with varying feature width."""
    num_entities = 1000

    client = MongoClient(mongodb_connection_string)
    try:
        client["benchmark_db"]["feature_history"].drop()
        now = _generate_native_data(
            client,
            "benchmark_db",
            "feature_history",
            "driver_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=3,
        )

        _, fv = _create_native_fv(num_features)

        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        feature_refs = [f"driver_benchmark:feature_{i}" for i in range(num_features)]

        def run_query():
            job = MongoDBOfflineStoreOne.get_historical_features(
                config=native_config,
                feature_views=[fv],
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return job.to_df()

        result = _run_benchmark_full(run_query, mongo_client=client)
        _print_benchmark_result(
            "NATIVE", "Features", num_features, result, num_rows=num_entities
        )

    finally:
        client.close()


# =============================================================================
# Test 3: Skewed Entity Distribution
# =============================================================================


@_requires_docker
@pytest.mark.parametrize("unique_ratio", [1.0, 0.5, 0.1])  # 100%, 50%, 10% unique
def test_entity_skew_ibis(
    mongodb_connection_string: str, ibis_config: RepoConfig, unique_ratio: float
) -> None:
    """Benchmark Ibis with varying entity uniqueness in entity_df."""
    import numpy as np

    total_rows = 5000
    num_features = 10
    num_unique_entities = int(total_rows * unique_ratio)
    num_unique_entities = max(num_unique_entities, 1)

    client = MongoClient(mongodb_connection_string)
    try:
        now = _generate_ibis_data(
            client,
            "benchmark_db",
            "driver_benchmark",
            num_entities=num_unique_entities,
            num_features=num_features,
            rows_per_entity=5,
        )

        _, fv = _create_ibis_fv(num_features)

        # Create entity_df with repeated entity_ids
        entity_ids = np.random.choice(
            num_unique_entities, size=total_rows, replace=True
        )
        entity_df = pd.DataFrame(
            {
                "driver_id": entity_ids,
                "event_timestamp": [
                    now - timedelta(minutes=i % 60) for i in range(total_rows)
                ],
            }
        )

        feature_refs = [f"driver_benchmark:feature_{i}" for i in range(num_features)]

        def run_query():
            job = MongoDBOfflineStoreMany.get_historical_features(
                config=ibis_config,
                feature_views=[fv],
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return job.to_df()

        result = _run_benchmark_full(run_query, mongo_client=client)
        print(
            f"\n[IBIS] Unique ratio: {unique_ratio:.0%} ({num_unique_entities:,} unique / {total_rows:,} rows)"
        )
        print(f"  Time:   {result.elapsed_seconds:.3f}s")
        print(f"  Memory: {result.peak_memory_mb:.1f} MB")
        print(f"  Mongo ops: {result.mongo_opcounters_delta}")

    finally:
        client.close()


@_requires_docker
@pytest.mark.parametrize("unique_ratio", [1.0, 0.5, 0.1])
def test_entity_skew_native(
    mongodb_connection_string: str, native_config: RepoConfig, unique_ratio: float
) -> None:
    """Benchmark Native with varying entity uniqueness in entity_df."""
    import numpy as np

    total_rows = 5000
    num_features = 10
    num_unique_entities = int(total_rows * unique_ratio)
    num_unique_entities = max(num_unique_entities, 1)

    client = MongoClient(mongodb_connection_string)
    try:
        client["benchmark_db"]["feature_history"].drop()
        now = _generate_native_data(
            client,
            "benchmark_db",
            "feature_history",
            "driver_benchmark",
            num_entities=num_unique_entities,
            num_features=num_features,
            rows_per_entity=5,
        )

        _, fv = _create_native_fv(num_features)

        entity_ids = np.random.choice(
            num_unique_entities, size=total_rows, replace=True
        )
        entity_df = pd.DataFrame(
            {
                "driver_id": entity_ids,
                "event_timestamp": [
                    now - timedelta(minutes=i % 60) for i in range(total_rows)
                ],
            }
        )

        feature_refs = [f"driver_benchmark:feature_{i}" for i in range(num_features)]

        def run_query():
            job = MongoDBOfflineStoreOne.get_historical_features(
                config=native_config,
                feature_views=[fv],
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return job.to_df()

        result = _run_benchmark_full(run_query, mongo_client=client)
        print(
            f"\n[NATIVE] Unique ratio: {unique_ratio:.0%} ({num_unique_entities:,} unique / {total_rows:,} rows)"
        )
        print(f"  Time:   {result.elapsed_seconds:.3f}s")
        print(f"  Memory: {result.peak_memory_mb:.1f} MB")
        print(f"  Mongo ops: {result.mongo_opcounters_delta}")

    finally:
        client.close()


# =============================================================================
# Summary comparison test
# =============================================================================


@_requires_docker
def test_summary_comparison(
    mongodb_connection_string: str, ibis_config: RepoConfig, native_config: RepoConfig
) -> None:
    """Run a standard comparison and print summary with full metrics."""
    num_entities = 2000
    num_features = 20

    client = MongoClient(mongodb_connection_string)
    try:
        # Setup Ibis data
        now = _generate_ibis_data(
            client,
            "benchmark_db",
            "driver_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=5,
        )

        # Setup Native data
        client["benchmark_db"]["feature_history"].drop()
        _generate_native_data(
            client,
            "benchmark_db",
            "feature_history",
            "driver_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=5,
        )

        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        feature_refs = [f"driver_benchmark:feature_{i}" for i in range(num_features)]

        # Ibis benchmark
        _, ibis_fv = _create_ibis_fv(num_features)

        def run_ibis():
            job = MongoDBOfflineStoreMany.get_historical_features(
                config=ibis_config,
                feature_views=[ibis_fv],
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return job.to_df()

        ibis_result = _run_benchmark_full(run_ibis, mongo_client=client)

        # Native benchmark
        _, native_fv = _create_native_fv(num_features)

        def run_native():
            job = MongoDBOfflineStoreOne.get_historical_features(
                config=native_config,
                feature_views=[native_fv],
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return job.to_df()

        native_result = _run_benchmark_full(run_native, mongo_client=client)

        # Print summary
        print("\n" + "=" * 70)
        print("SUMMARY COMPARISON")
        print("=" * 70)
        print(f"Entities: {num_entities:,} | Features: {num_features}")
        print("-" * 70)
        print(f"{'Metric':<20} {'Ibis':>20} {'Native':>20}")
        print("-" * 70)
        print(
            f"{'Time (s)':<20} {ibis_result.elapsed_seconds:>20.3f} {native_result.elapsed_seconds:>20.3f}"
        )
        print(
            f"{'Memory (MB)':<20} {ibis_result.peak_memory_mb:>20.1f} {native_result.peak_memory_mb:>20.1f}"
        )
        print(
            f"{'Rows/sec':<20} {num_entities / ibis_result.elapsed_seconds:>20,.0f} {num_entities / native_result.elapsed_seconds:>20,.0f}"
        )
        print("-" * 70)

        if native_result.elapsed_seconds > 0:
            ratio = native_result.elapsed_seconds / ibis_result.elapsed_seconds
            print(f"Ibis is {ratio:.1f}x faster than Native")
        print("=" * 70)

    finally:
        client.close()
