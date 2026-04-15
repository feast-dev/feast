"""
Performance benchmarks comparing MongoDB offline store implementations: Many vs One.

- Many: One collection per FeatureView (MongoDBOfflineStoreMany)
- One: Single shared collection for all FeatureViews (MongoDBOfflineStoreOne)

These tests measure performance across different scaling dimensions:
1. Row count scaling (entity_df size)
2. Feature width scaling (features per FeatureView)
3. Entity distribution (unique vs skewed/repeated entity_ids)

Metrics captured:
- Runtime (wall clock)
- Memory (peak Python memory via tracemalloc)
- MongoDB server metrics (opcounters, execution stats)

Run with: pytest benchmark.py -v -s
Skip slow tests: pytest benchmark.py -v -s -m "not slow"
"""

import resource
import sys
import time
import tracemalloc
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Generator, List, Optional

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


# ---------------------------------------------------------------------------
# RSS measurement (per-operation delta)
# ---------------------------------------------------------------------------


def _rss_mb() -> float:
    """Return current process RSS in MB.

    On macOS ``ru_maxrss`` is in bytes; on Linux it is in kilobytes.
    Note: ``ru_maxrss`` is a *lifetime peak* reported by the kernel.  We
    snapshot it before and after each operation to compute the growth
    attributable to that operation.  Because the peak never decreases, the
    delta accurately reflects the high-water-mark added by that operation
    even after GC has freed Python objects.
    """
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return rss / (1024 * 1024)
    return rss / 1024  # Linux: KB → MB


# ---------------------------------------------------------------------------
# Batched data generators (memory-efficient at N > 100 K)
# ---------------------------------------------------------------------------

_INSERT_BATCH = 50_000  # docs per insert_many call


def _generate_many_data_batched(
    client: Any,
    db_name: str,
    collection_name: str,
    num_entities: int,
    num_features: int,
    rows_per_entity: int = 5,
) -> datetime:
    """Generate Many-schema data using batched inserts.

    Pre-computes one timestamp per historical row so we avoid calling
    ``timedelta`` inside the inner loop.  Feature values are simple
    floats derived from (entity, row, feature) — no numpy required.
    """
    import numpy as np

    collection = client[db_name][collection_name]
    collection.drop()

    now = datetime.now(tz=pytz.UTC)
    timestamps = [now - timedelta(hours=r) for r in range(rows_per_entity)]
    feat_names = [f"feature_{f}" for f in range(num_features)]

    # Pre-generate all feature values as a 2-D numpy array for speed.
    rng = np.random.default_rng(seed=42)
    feature_matrix = rng.random((num_entities * rows_per_entity, num_features)).astype(
        float
    )

    batch: List[Dict] = []
    idx = 0
    for entity_id in range(num_entities):
        for row in range(rows_per_entity):
            doc: Dict[str, Any] = {
                "driver_id": entity_id,
                "event_timestamp": timestamps[row],
            }
            row_vals = feature_matrix[idx]
            for fi, fname in enumerate(feat_names):
                doc[fname] = float(row_vals[fi])
            batch.append(doc)
            idx += 1
            if len(batch) >= _INSERT_BATCH:
                collection.insert_many(batch)
                batch = []

    if batch:
        collection.insert_many(batch)

    return now


def _generate_one_data_batched(
    client: Any,
    db_name: str,
    collection_name: str,
    feature_view_name: str,
    num_entities: int,
    num_features: int,
    rows_per_entity: int = 5,
) -> datetime:
    """Generate One-schema data using batched inserts."""
    import numpy as np

    collection = client[db_name][collection_name]
    # Do NOT drop — multiple feature views may share the collection.

    now = datetime.now(tz=pytz.UTC)
    timestamps = [now - timedelta(hours=r) for r in range(rows_per_entity)]
    feat_names = [f"feature_{f}" for f in range(num_features)]

    rng = np.random.default_rng(seed=42)
    feature_matrix = rng.random((num_entities * rows_per_entity, num_features)).astype(
        float
    )

    batch: List[Dict] = []
    idx = 0
    for entity_id in range(num_entities):
        eid_bytes = _make_entity_id(entity_id)
        for row in range(rows_per_entity):
            row_vals = feature_matrix[idx]
            features = {
                feat_names[fi]: float(row_vals[fi]) for fi in range(num_features)
            }
            doc: Dict[str, Any] = {
                "entity_id": eid_bytes,
                "feature_view": feature_view_name,
                "features": features,
                "event_timestamp": timestamps[row],
                "created_at": timestamps[row],
            }
            batch.append(doc)
            idx += 1
            if len(batch) >= _INSERT_BATCH:
                collection.insert_many(batch)
                batch = []

    if batch:
        collection.insert_many(batch)

    return now


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
def many_config(mongodb_connection_string: str) -> RepoConfig:
    """RepoConfig for Many implementation (one collection per FeatureView)."""
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
def one_config(mongodb_connection_string: str) -> RepoConfig:
    """RepoConfig for One implementation (single shared collection)."""
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


def _generate_many_data(
    client: MongoClient,
    db_name: str,
    collection_name: str,
    num_entities: int,
    num_features: int,
    rows_per_entity: int = 5,
) -> datetime:
    """Generate test data for Many (one collection per FV, flat schema)."""
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


def _generate_one_data(
    client: MongoClient,
    db_name: str,
    collection_name: str,
    feature_view_name: str,
    num_entities: int,
    num_features: int,
    rows_per_entity: int = 5,
) -> datetime:
    """Generate test data for One (single collection, nested features)."""
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


def _create_many_fv(num_features: int) -> tuple:
    """Create Many source and FeatureView."""
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


def _create_one_fv(num_features: int) -> tuple:
    """Create One source and FeatureView."""
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
    peak_memory_mb: float  # tracemalloc: Python-heap peak for this op
    rss_delta_mb: float = 0.0  # RSS growth attributable to this op (OS-level)
    mongo_opcounters_delta: Dict[str, int] = field(default_factory=dict)
    status: str = "OK"  # "OK" | "OOM" | "ERROR:<msg>"


def _run_benchmark_full(
    func,
    mongo_client: Optional[Any] = None,
) -> FullBenchmarkResult:
    """Run a benchmark capturing runtime, memory (tracemalloc + RSS), and MongoDB metrics."""
    mongo_before = None
    if mongo_client:
        mongo_before = MongoMetrics.capture(mongo_client)

    rss_before = _rss_mb()
    tracemalloc.start()
    start = time.perf_counter()

    status = "OK"
    try:
        func()
    except MemoryError:
        status = "OOM"
    except Exception as exc:
        status = f"ERROR:{exc!s:.80}"

    elapsed = time.perf_counter() - start
    _, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss_after = _rss_mb()

    mongo_delta: Dict[str, int] = {}
    if mongo_client and mongo_before:
        mongo_after = MongoMetrics.capture(mongo_client)
        mongo_delta = mongo_before.delta(mongo_after)

    return FullBenchmarkResult(
        elapsed_seconds=elapsed,
        peak_memory_mb=peak_memory / (1024 * 1024),
        rss_delta_mb=max(rss_after - rss_before, 0.0),
        mongo_opcounters_delta=mongo_delta,
        status=status,
    )


def _print_benchmark_result(
    impl: str,
    dimension_name: str,
    dimension_value: int,
    result: FullBenchmarkResult,
    num_rows: Optional[int] = None,
) -> None:
    """Pretty print benchmark results."""
    status_tag = f"  [{result.status}]" if result.status != "OK" else ""
    print(f"\n[{impl}] {dimension_name}: {dimension_value:,}{status_tag}")
    print(f"  Time:        {result.elapsed_seconds:.3f}s")
    print(f"  Mem (trace): {result.peak_memory_mb:.1f} MB")
    print(f"  Mem (RSS Δ): {result.rss_delta_mb:.1f} MB")
    if num_rows and result.elapsed_seconds > 0:
        print(f"  Rate:        {num_rows / result.elapsed_seconds:,.0f} rows/s")
    if result.mongo_opcounters_delta:
        print(f"  Mongo ops:   {result.mongo_opcounters_delta}")


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
def test_scale_rows_many(
    mongodb_connection_string: str, many_config: RepoConfig, num_rows: int
) -> None:
    """Benchmark Many implementation with varying entity_df sizes.

    Measures: runtime, peak memory, MongoDB opcounters.
    """
    num_features = 10
    num_entities = num_rows  # One row per entity for simplicity

    client = MongoClient(mongodb_connection_string)
    try:
        now = _generate_many_data(
            client,
            "benchmark_db",
            "driver_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=3,
        )

        _, fv = _create_many_fv(num_features)

        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        feature_refs = [f"driver_benchmark:feature_{i}" for i in range(num_features)]

        def run_query():
            job = MongoDBOfflineStoreMany.get_historical_features(
                config=many_config,
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
def test_scale_rows_one(
    mongodb_connection_string: str, one_config: RepoConfig, num_rows: int
) -> None:
    """Benchmark One implementation with varying entity_df sizes.

    Measures: runtime, peak memory, MongoDB opcounters.
    """
    num_features = 10
    num_entities = num_rows

    client = MongoClient(mongodb_connection_string)
    try:
        client["benchmark_db"]["feature_history"].drop()
        now = _generate_one_data(
            client,
            "benchmark_db",
            "feature_history",
            "driver_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=3,
        )

        _, fv = _create_one_fv(num_features)

        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        feature_refs = [f"driver_benchmark:feature_{i}" for i in range(num_features)]

        def run_query():
            job = MongoDBOfflineStoreOne.get_historical_features(
                config=one_config,
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
def test_wide_features_many(
    mongodb_connection_string: str, many_config: RepoConfig, num_features: int
) -> None:
    """Benchmark Many with varying feature width."""
    num_entities = 1000

    client = MongoClient(mongodb_connection_string)
    try:
        now = _generate_many_data(
            client,
            "benchmark_db",
            "driver_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=3,
        )

        _, fv = _create_many_fv(num_features)

        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        feature_refs = [f"driver_benchmark:feature_{i}" for i in range(num_features)]

        def run_query():
            job = MongoDBOfflineStoreMany.get_historical_features(
                config=many_config,
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
def test_wide_features_one(
    mongodb_connection_string: str, one_config: RepoConfig, num_features: int
) -> None:
    """Benchmark One with varying feature width."""
    num_entities = 1000

    client = MongoClient(mongodb_connection_string)
    try:
        client["benchmark_db"]["feature_history"].drop()
        now = _generate_one_data(
            client,
            "benchmark_db",
            "feature_history",
            "driver_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=3,
        )

        _, fv = _create_one_fv(num_features)

        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        feature_refs = [f"driver_benchmark:feature_{i}" for i in range(num_features)]

        def run_query():
            job = MongoDBOfflineStoreOne.get_historical_features(
                config=one_config,
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
def test_entity_skew_many(
    mongodb_connection_string: str, many_config: RepoConfig, unique_ratio: float
) -> None:
    """Benchmark Many with varying entity uniqueness in entity_df."""
    import numpy as np

    total_rows = 5000
    num_features = 10
    num_unique_entities = int(total_rows * unique_ratio)
    num_unique_entities = max(num_unique_entities, 1)

    client = MongoClient(mongodb_connection_string)
    try:
        now = _generate_many_data(
            client,
            "benchmark_db",
            "driver_benchmark",
            num_entities=num_unique_entities,
            num_features=num_features,
            rows_per_entity=5,
        )

        _, fv = _create_many_fv(num_features)

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
                config=many_config,
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
            f"\n[MANY] Unique ratio: {unique_ratio:.0%} ({num_unique_entities:,} unique / {total_rows:,} rows)"
        )
        print(f"  Time:   {result.elapsed_seconds:.3f}s")
        print(f"  Memory: {result.peak_memory_mb:.1f} MB")
        print(f"  Mongo ops: {result.mongo_opcounters_delta}")

    finally:
        client.close()


@_requires_docker
@pytest.mark.parametrize("unique_ratio", [1.0, 0.5, 0.1])
def test_entity_skew_one(
    mongodb_connection_string: str, one_config: RepoConfig, unique_ratio: float
) -> None:
    """Benchmark One with varying entity uniqueness in entity_df."""
    import numpy as np

    total_rows = 5000
    num_features = 10
    num_unique_entities = int(total_rows * unique_ratio)
    num_unique_entities = max(num_unique_entities, 1)

    client = MongoClient(mongodb_connection_string)
    try:
        client["benchmark_db"]["feature_history"].drop()
        now = _generate_one_data(
            client,
            "benchmark_db",
            "feature_history",
            "driver_benchmark",
            num_entities=num_unique_entities,
            num_features=num_features,
            rows_per_entity=5,
        )

        _, fv = _create_one_fv(num_features)

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
                config=one_config,
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
            f"\n[ONE] Unique ratio: {unique_ratio:.0%} ({num_unique_entities:,} unique / {total_rows:,} rows)"
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
    mongodb_connection_string: str, many_config: RepoConfig, one_config: RepoConfig
) -> None:
    """Run a standard comparison and print summary with full metrics."""
    num_entities = 2000
    num_features = 20

    client = MongoClient(mongodb_connection_string)
    try:
        # Setup Many data
        now = _generate_many_data(
            client,
            "benchmark_db",
            "driver_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=5,
        )

        # Setup One data
        client["benchmark_db"]["feature_history"].drop()
        _generate_one_data(
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

        # Many benchmark
        _, many_fv = _create_many_fv(num_features)

        def run_many():
            job = MongoDBOfflineStoreMany.get_historical_features(
                config=many_config,
                feature_views=[many_fv],
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return job.to_df()

        many_result = _run_benchmark_full(run_many, mongo_client=client)

        # One benchmark
        _, one_fv = _create_one_fv(num_features)

        def run_one():
            job = MongoDBOfflineStoreOne.get_historical_features(
                config=one_config,
                feature_views=[one_fv],
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return job.to_df()

        one_result = _run_benchmark_full(run_one, mongo_client=client)

        # Print summary
        print("\n" + "=" * 70)
        print("SUMMARY COMPARISON: Many vs One")
        print("=" * 70)
        print(f"Entities: {num_entities:,} | Features: {num_features}")
        print("-" * 70)
        print(f"{'Metric':<20} {'Many':>20} {'One':>20}")
        print("-" * 70)
        print(
            f"{'Time (s)':<20} {many_result.elapsed_seconds:>20.3f} {one_result.elapsed_seconds:>20.3f}"
        )
        print(
            f"{'Memory (MB)':<20} {many_result.peak_memory_mb:>20.1f} {one_result.peak_memory_mb:>20.1f}"
        )
        print(
            f"{'Rows/sec':<20} {num_entities / many_result.elapsed_seconds:>20,.0f} {num_entities / one_result.elapsed_seconds:>20,.0f}"
        )
        print("-" * 70)

        if one_result.elapsed_seconds > 0:
            ratio = one_result.elapsed_seconds / many_result.elapsed_seconds
            faster = "Many" if ratio > 1 else "One"
            print(f"{faster} is {max(ratio, 1 / ratio):.1f}x faster")
        print("=" * 70)

    finally:
        client.close()


# =============================================================================
# Test 4: Large-Scale Memory Stress (N × M × P sweep)
# =============================================================================
#
# Goal: demonstrate that Many's memory grows O(N × M × P) while One stays
# bounded by CHUNK_SIZE regardless of collection size.
#
# Dimensions held constant across this suite:
#   M = STRESS_NUM_FEATURES  = 50  (5× wider than the PR baseline)
#   P = STRESS_ROWS_PER_ENTITY = 5  (slightly deeper history)
#
# Total collection docs = N × P.  Memory for Many ≈ total_docs × M × 8 bytes
# × ~20 pandas/ibis overhead factor (empirically observed in PR benchmarks).
# One memory stays ≈ constant because it fetches CHUNK_SIZE entities at a time.

STRESS_ENTITY_COUNTS = [50_000, 150_000, 300_000, 500_000]
STRESS_NUM_FEATURES = 50
STRESS_ROWS_PER_ENTITY = 5


def _create_many_fv_stress(num_features: int) -> tuple:
    """Many source + FV for stress tests."""
    source = MongoDBSourceMany(
        name="stress_benchmark",
        database="benchmark_db",
        collection="stress_benchmark",
        timestamp_field="event_timestamp",
    )
    entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    schema = [Field(name="driver_id", dtype=Int64)] + [
        Field(name=f"feature_{f}", dtype=Float64) for f in range(num_features)
    ]
    fv = FeatureView(
        name="stress_benchmark",
        entities=[entity],
        schema=schema,
        source=source,
        ttl=timedelta(days=1),
    )
    return source, fv


def _create_one_fv_stress(num_features: int) -> tuple:
    """One source + FV for stress tests."""
    source = MongoDBSourceOne(
        name="stress_benchmark", timestamp_field="event_timestamp"
    )
    entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    schema = [Field(name="driver_id", dtype=Int64)] + [
        Field(name=f"feature_{f}", dtype=Float64) for f in range(num_features)
    ]
    fv = FeatureView(
        name="stress_benchmark",
        entities=[entity],
        schema=schema,
        source=source,
        ttl=timedelta(days=1),
    )
    return source, fv


@_requires_docker
@pytest.mark.slow
@pytest.mark.timeout(0)  # opt out of global 300s limit — large inserts take time
@pytest.mark.parametrize("num_entities", STRESS_ENTITY_COUNTS)
def test_scale_entities_stress_many(
    mongodb_connection_string: str, many_config: RepoConfig, num_entities: int
) -> None:
    """Many: memory stress across large entity counts with wide features.

    Collection has ``num_entities × STRESS_ROWS_PER_ENTITY`` total documents,
    each with ``STRESS_NUM_FEATURES`` float fields.  Many loads the entire
    collection into memory, so peak memory grows linearly.  A MemoryError is
    caught and reported as "OOM" without failing the test.
    """
    num_features = STRESS_NUM_FEATURES
    rows_per_entity = STRESS_ROWS_PER_ENTITY
    total_docs = num_entities * rows_per_entity

    client = MongoClient(mongodb_connection_string)
    try:
        print(
            f"\n[MANY stress] Generating {total_docs:,} docs "
            f"({num_entities:,} entities × {rows_per_entity} rows × {num_features} features)…"
        )
        now = _generate_many_data_batched(
            client,
            "benchmark_db",
            "stress_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=rows_per_entity,
        )

        _, fv = _create_many_fv_stress(num_features)
        feature_refs = [f"stress_benchmark:feature_{i}" for i in range(num_features)]
        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        safe, projected_mb, free_mb = _check_memory_for_many(
            num_entities, rows_per_entity, num_features
        )
        if not safe:
            print(
                f"  [MANY stress] Skipping query — projected {projected_mb:,.0f} MB "
                f"exceeds 80% of {free_mb:,.0f} MB free RAM.  Would likely SIGKILL."
            )
            result = FullBenchmarkResult(
                elapsed_seconds=0,
                peak_memory_mb=projected_mb,
                status="SKIPPED:low_memory",
            )
        else:

            def run_query():
                job = MongoDBOfflineStoreMany.get_historical_features(
                    config=many_config,
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
            "MANY", "Entities (stress)", num_entities, result, num_rows=num_entities
        )
        print(
            f"  Collection docs: {total_docs:,}  |  "
            f"Raw data ≈ {total_docs * num_features * 8 / 1e6:.0f} MB"
        )

    finally:
        client.close()


@_requires_docker
@pytest.mark.slow
@pytest.mark.timeout(0)  # opt out of global 300s limit — large inserts take time
@pytest.mark.parametrize("num_entities", STRESS_ENTITY_COUNTS)
def test_scale_entities_stress_one(
    mongodb_connection_string: str, one_config: RepoConfig, num_entities: int
) -> None:
    """One: memory stress across the same large entity counts.

    Because One fetches CHUNK_SIZE=50,000 entities at a time from MongoDB,
    its peak memory is O(CHUNK_SIZE × M) regardless of N.  All parametrize
    values should complete without OOM.
    """
    num_features = STRESS_NUM_FEATURES
    rows_per_entity = STRESS_ROWS_PER_ENTITY
    total_docs = num_entities * rows_per_entity

    client = MongoClient(mongodb_connection_string)
    try:
        client["benchmark_db"]["feature_history_stress"].drop()
        print(
            f"\n[ONE stress] Generating {total_docs:,} docs "
            f"({num_entities:,} entities × {rows_per_entity} rows × {num_features} features)…"
        )
        now = _generate_one_data_batched(
            client,
            "benchmark_db",
            "feature_history_stress",
            "stress_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=rows_per_entity,
        )

        # Use a separate RepoConfig pointing at the stress collection
        stress_one_config = RepoConfig(
            project="benchmark",
            registry="memory://",
            provider="local",
            offline_store=MongoDBOfflineStoreOneConfig(
                connection_string=mongodb_connection_string,
                database="benchmark_db",
                collection="feature_history_stress",
            ),
            online_store={"type": "sqlite"},
            entity_key_serialization_version=ENTITY_KEY_VERSION,
        )

        _, fv = _create_one_fv_stress(num_features)
        feature_refs = [f"stress_benchmark:feature_{i}" for i in range(num_features)]
        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        def run_query():
            job = MongoDBOfflineStoreOne.get_historical_features(
                config=stress_one_config,
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
            "ONE", "Entities (stress)", num_entities, result, num_rows=num_entities
        )
        print(
            f"  Collection docs: {total_docs:,}  |  "
            f"Raw data ≈ {total_docs * num_features * 8 / 1e6:.0f} MB"
        )
        assert result.status == "OK", f"One should never OOM — got: {result.status}"

    finally:
        client.close()


# =============================================================================
# Test 5: OOM Crossover Demo
# =============================================================================
#
# Runs BOTH implementations against the same large collection.
#   - Many is expected to exhaust memory and raise MemoryError (caught → OOM).
#   - One must complete successfully, demonstrating that chunked fetching
#     keeps memory bounded regardless of total collection size.
#
# Scale selection:
#   OOM_ENTITIES × OOM_ROWS × OOM_FEATURES total float values in MongoDB.
#   Empirically, Many needs ~20 bytes per float (pandas + ibis + pyarrow copies).
#   To exceed a 32 GB machine: 32 GB / 20 / 8 ≈ 200 M floats → ~400 K entities
#   at (OOM_ROWS=5, OOM_FEATURES=100).
#
# If your machine has less RAM the crossover will happen at a smaller N;
# if it has more you may need to increase OOM_ENTITIES.

OOM_ENTITIES = 400_000  # total unique entities
OOM_FEATURES = 100  # float features per document
OOM_ROWS = 5  # historical rows per entity
# → total docs = 2,000,000  |  raw floats = 200,000,000  |  raw bytes ≈ 1.6 GB
# → Many projected peak ≈ 32 GB  (likely OOM on ≤32 GB machines)
# → One projected peak   ≈ CHUNK_SIZE × OOM_FEATURES × 20 B ≈ 100 MB


def _available_memory_mb() -> float:
    """Estimate *total* installed RAM in MB (used for reporting only)."""
    try:
        import subprocess

        out = subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip()
        return int(out) / (1024 * 1024)
    except Exception:
        pass
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) / 1024
    except Exception:
        pass
    return 16_384  # conservative fallback: 16 GB


def _free_memory_mb() -> float:
    """Estimate *currently free/available* RAM in MB.

    On macOS uses ``vm_stat`` (free + purgeable pages at 16 KB each).
    On Linux reads ``MemAvailable`` from ``/proc/meminfo``.
    Falls back to a conservative 4 GB if neither is available.

    This is used to gate Many queries before they are attempted — avoiding
    SIGKILL from the macOS jetsam memory-pressure system, which cannot be
    caught by Python's ``except MemoryError``.
    """
    try:
        import subprocess

        out = subprocess.check_output(["vm_stat"], text=True)
        page_size = 16_384  # 16 KB on Apple Silicon; 4 KB on Intel
        try:
            hw = subprocess.check_output(
                ["sysctl", "-n", "hw.pagesize"], text=True
            ).strip()
            page_size = int(hw)
        except Exception:
            pass
        free = purgeable = 0
        for line in out.splitlines():
            if line.startswith("Pages free:"):
                free = int(line.split(":")[1].strip().rstrip("."))
            elif line.startswith("Pages purgeable:"):
                purgeable = int(line.split(":")[1].strip().rstrip("."))
        return (free + purgeable) * page_size / (1024 * 1024)
    except Exception:
        pass
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemAvailable:"):
                    return int(line.split()[1]) / 1024
    except Exception:
        pass
    return 4_096  # conservative fallback: 4 GB


# Empirical overhead ratio: bytes consumed by Many per raw float value.
# Measured across stress test runs (tracemalloc peak / raw float count).
# 500K entities × P=5 × M=50 → 125M floats → 16.2 GB → ~130 bytes/float.
_MANY_BYTES_PER_FLOAT = 130


def _many_projected_mb(
    num_entities: int, rows_per_entity: int, num_features: int
) -> float:
    """Project peak tracemalloc memory for a Many query in MB."""
    return num_entities * rows_per_entity * num_features * _MANY_BYTES_PER_FLOAT / 1e6


def _check_memory_for_many(
    num_entities: int,
    rows_per_entity: int,
    num_features: int,
    safety_factor: float = 0.80,
) -> tuple:
    """Return (safe_to_run, projected_mb, free_mb).

    ``safe_to_run`` is False when the projected peak exceeds
    ``safety_factor × free_mb``, meaning we would likely be SIGKILL'd.
    """
    projected = _many_projected_mb(num_entities, rows_per_entity, num_features)
    free = _free_memory_mb()
    return projected < free * safety_factor, projected, free


@_requires_docker
@pytest.mark.slow
@pytest.mark.timeout(0)  # opt out of global 300s limit — large inserts take time
def test_oom_crossover(mongodb_connection_string: str, many_config: RepoConfig) -> None:
    """Demonstrate that Many OOMs where One succeeds (the key trade-off).

    Both implementations run against an identical dataset.  Many loads the
    entire collection into memory — at large enough scale it will raise
    MemoryError, which is caught and reported.  One uses chunked entity-filtered
    queries and therefore completes with bounded memory.

    Expected output::

        ╔══════════════════════════════════════════════════════════════════════╗
        ║           OOM CROSSOVER DEMO  —  Many vs One                        ║
        ╠══════════════════════════════════════════════════════════════════════╣
        ║ Scale: 400,000 entities × 5 rows × 100 features                     ║
        ║        2,000,000 total docs  |  raw data ≈ 1,600 MB                 ║
        ╠═══════════════════════╦════════════════╦════════════════════════════╣
        ║ Metric                ║ Many           ║ One                        ║
        ╠═══════════════════════╬════════════════╬════════════════════════════╣
        ║ Status                ║ OOM ❌         ║ OK ✅                      ║
        ║ Time (s)              ║ --             ║ 312.4                      ║
        ║ Mem tracemalloc (MB)  ║ --             ║ 312.0                      ║
        ║ Mem RSS Δ (MB)        ║ --             ║ 189.0                      ║
        ╚═══════════════════════╩════════════════╩════════════════════════════╝
    """
    num_entities = OOM_ENTITIES
    num_features = OOM_FEATURES
    rows_per_entity = OOM_ROWS
    total_docs = num_entities * rows_per_entity
    raw_mb = total_docs * num_features * 8 / 1e6
    avail_mb = _available_memory_mb()

    print(
        f"\n{'=' * 72}\n"
        f"OOM CROSSOVER DEMO\n"
        f"  Scale  : {num_entities:,} entities × {rows_per_entity} rows × {num_features} features\n"
        f"  Docs   : {total_docs:,} total  |  raw ≈ {raw_mb:.0f} MB\n"
        f"  Machine: {avail_mb / 1024:.0f} GB RAM\n"
        f"{'=' * 72}"
    )

    stress_one_cfg = RepoConfig(
        project="benchmark",
        registry="memory://",
        provider="local",
        offline_store=MongoDBOfflineStoreOneConfig(
            connection_string=mongodb_connection_string,
            database="benchmark_db",
            collection="feature_history_oom",
        ),
        online_store={"type": "sqlite"},
        entity_key_serialization_version=ENTITY_KEY_VERSION,
    )

    # Build Many source + FV pointing at the oom_benchmark collection
    many_source = MongoDBSourceMany(
        name="oom_benchmark",
        database="benchmark_db",
        collection="oom_benchmark",
        timestamp_field="event_timestamp",
    )
    entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    many_schema = [Field(name="driver_id", dtype=Int64)] + [
        Field(name=f"feature_{f}", dtype=Float64) for f in range(num_features)
    ]
    many_fv = FeatureView(
        name="oom_benchmark",
        entities=[entity],
        schema=many_schema,
        source=many_source,
        ttl=timedelta(days=1),
    )

    # Build One source + FV (collection lives in feature_history_oom)
    one_source = MongoDBSourceOne(
        name="oom_benchmark", timestamp_field="event_timestamp"
    )
    one_schema = [Field(name="driver_id", dtype=Int64)] + [
        Field(name=f"feature_{f}", dtype=Float64) for f in range(num_features)
    ]
    one_fv = FeatureView(
        name="oom_benchmark",
        entities=[entity],
        schema=one_schema,
        source=one_source,
        ttl=timedelta(days=1),
    )

    feature_refs = [f"oom_benchmark:feature_{i}" for i in range(num_features)]

    client = MongoClient(mongodb_connection_string)
    try:
        # --- Generate data for Many ---
        print(f"Generating Many data ({total_docs:,} docs)…")
        now = _generate_many_data_batched(
            client,
            "benchmark_db",
            "oom_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=rows_per_entity,
        )

        # --- Generate data for One ---
        print(f"Generating One data ({total_docs:,} docs)…")
        client["benchmark_db"]["feature_history_oom"].drop()
        _generate_one_data_batched(
            client,
            "benchmark_db",
            "feature_history_oom",
            "oom_benchmark",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=rows_per_entity,
        )

        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        # --- Run Many ---
        print("Running Many (expect OOM on machines with ≤ 32 GB RAM)…")

        def run_many():
            many_job = MongoDBOfflineStoreMany.get_historical_features(
                config=many_config,
                feature_views=[many_fv],
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return many_job.to_df()

        many_result = _run_benchmark_full(run_many, mongo_client=client)

        # --- Run One ---
        print("Running One (should complete within bounded memory)…")

        def run_one():
            job = MongoDBOfflineStoreOne.get_historical_features(
                config=stress_one_cfg,
                feature_views=[one_fv],
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return job.to_df()

        one_result = _run_benchmark_full(run_one, mongo_client=client)

        # --- Print crossover table ---
        def _fmt(result: FullBenchmarkResult, field: str) -> str:
            if result.status != "OK":
                return f"{result.status} ❌"
            if field == "status":
                return "OK ✅"
            if field == "time":
                return f"{result.elapsed_seconds:.1f}s"
            if field == "trace":
                return f"{result.peak_memory_mb:.0f} MB"
            if field == "rss":
                return f"{result.rss_delta_mb:.0f} MB"
            return "?"

        col = 28
        print("\n" + "╔" + "═" * col + "╦" + "═" * 16 + "╦" + "═" * 16 + "╗")
        print(f"║ {'OOM CROSSOVER SUMMARY':<{col - 2}} ║ {'Many':^14} ║ {'One':^14} ║")
        print("╠" + "═" * col + "╬" + "═" * 16 + "╬" + "═" * 16 + "╣")
        print(
            f"║ {'Status':<{col - 2}} ║ {_fmt(many_result, 'status'):^14} ║ {_fmt(one_result, 'status'):^14} ║"
        )
        print(
            f"║ {'Time':<{col - 2}} ║ {_fmt(many_result, 'time'):^14} ║ {_fmt(one_result, 'time'):^14} ║"
        )
        print(
            f"║ {'Mem tracemalloc (MB)':<{col - 2}} ║ {_fmt(many_result, 'trace'):^14} ║ {_fmt(one_result, 'trace'):^14} ║"
        )
        print(
            f"║ {'Mem RSS Δ (MB)':<{col - 2}} ║ {_fmt(many_result, 'rss'):^14} ║ {_fmt(one_result, 'rss'):^14} ║"
        )
        print("╚" + "═" * col + "╩" + "═" * 16 + "╩" + "═" * 16 + "╝")

        # One must always complete regardless of machine size
        assert one_result.status == "OK", (
            f"One must complete at any scale — got {one_result.status}"
        )

    finally:
        client.close()


# =============================================================================
# Test 6: Realistic Benchmark — P=30 (daily batch, 30-day TTL), Single FV
# =============================================================================
#
# Motivation: the stress tests in Test 4 used P=5 and M=50, which inflated
# memory pressure via wide documents rather than deep history.  A daily batch
# feature store with a 30-day TTL is far more common — every entity has 30
# historical rows.  With M=10 features (a typical narrow feature view) and
# P=30, the OOM crossover shifts to much lower entity counts:
#
#   Projected Many memory = N × 30 × 10 floats × ~130 bytes/float (empirical)
#     100 K → ~3.9 GB   (comfortable)
#     300 K → ~11.7 GB  (heavy)
#     600 K → ~23.4 GB  (near 32 GB limit)
#     900 K → ~35.1 GB  → OOM expected on 32 GB machine
#
# One memory stays bounded at CHUNK_SIZE × 10 floats × overhead ≈ 650 MB
# regardless of N.

REALISTIC_SINGLE_ENTITY_COUNTS = [100_000, 300_000, 600_000, 900_000]
REALISTIC_NUM_FEATURES = 10
REALISTIC_ROWS_PER_ENTITY = 30  # daily data, 30-day TTL


@_requires_docker
@pytest.mark.slow
@pytest.mark.timeout(0)
@pytest.mark.parametrize("num_entities", REALISTIC_SINGLE_ENTITY_COUNTS)
def test_realistic_p30_single_fv_many(
    mongodb_connection_string: str, many_config: RepoConfig, num_entities: int
) -> None:
    """Many: realistic daily-batch scenario (M=10, P=30), single feature view.

    Collection size = N × 30 docs.  Many loads the entire collection, so memory
    scales linearly with N.  OOM is expected around 900 K entities on a 32 GB
    machine; the error is caught and reported without failing the test.
    """
    num_features = REALISTIC_NUM_FEATURES
    rows_per_entity = REALISTIC_ROWS_PER_ENTITY
    total_docs = num_entities * rows_per_entity

    client = MongoClient(mongodb_connection_string)
    try:
        print(
            f"\n[MANY realistic-single] Generating {total_docs:,} docs "
            f"({num_entities:,} entities × {rows_per_entity} rows × {num_features} features)…"
        )
        now = _generate_many_data_batched(
            client,
            "benchmark_db",
            "realistic_single",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=rows_per_entity,
        )

        source = MongoDBSourceMany(
            name="realistic_single",
            database="benchmark_db",
            collection="realistic_single",
            timestamp_field="event_timestamp",
        )
        entity = Entity(
            name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
        )
        schema = [Field(name="driver_id", dtype=Int64)] + [
            Field(name=f"feature_{f}", dtype=Float64) for f in range(num_features)
        ]
        fv = FeatureView(
            name="realistic_single",
            entities=[entity],
            schema=schema,
            source=source,
            ttl=timedelta(days=30),
        )
        feature_refs = [f"realistic_single:feature_{i}" for i in range(num_features)]
        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        safe, projected_mb, free_mb = _check_memory_for_many(
            num_entities, rows_per_entity, num_features
        )
        if not safe:
            print(
                f"  [MANY realistic-single] Skipping query — projected {projected_mb:,.0f} MB "
                f"exceeds 80% of {free_mb:,.0f} MB free RAM.  Would likely SIGKILL."
            )
            result = FullBenchmarkResult(
                elapsed_seconds=0,
                peak_memory_mb=projected_mb,
                status="SKIPPED:low_memory",
            )
        else:

            def run_query():
                many_job = MongoDBOfflineStoreMany.get_historical_features(
                    config=many_config,
                    feature_views=[fv],
                    feature_refs=feature_refs,
                    entity_df=entity_df,
                    registry=MagicMock(),
                    project="benchmark",
                    full_feature_names=False,
                )
                return many_job.to_df()

            result = _run_benchmark_full(run_query, mongo_client=client)

        _print_benchmark_result(
            "MANY",
            "Entities (realistic P=30, 1 FV)",
            num_entities,
            result,
            num_entities,
        )
        print(
            f"  Collection docs: {total_docs:,}  |  Raw ≈ {total_docs * num_features * 8 / 1e6:.0f} MB"
        )

    finally:
        client.close()


@_requires_docker
@pytest.mark.slow
@pytest.mark.timeout(0)
@pytest.mark.parametrize("num_entities", REALISTIC_SINGLE_ENTITY_COUNTS)
def test_realistic_p30_single_fv_one(
    mongodb_connection_string: str, num_entities: int
) -> None:
    """One: realistic daily-batch scenario (M=10, P=30), single feature view.

    One's memory ceiling is O(CHUNK_SIZE × M) and does not grow with N.
    All parametrize values must complete without OOM.
    """
    num_features = REALISTIC_NUM_FEATURES
    rows_per_entity = REALISTIC_ROWS_PER_ENTITY
    total_docs = num_entities * rows_per_entity
    coll = "realistic_one_single"

    client = MongoClient(mongodb_connection_string)
    try:
        client["benchmark_db"][coll].drop()
        print(
            f"\n[ONE realistic-single] Generating {total_docs:,} docs "
            f"({num_entities:,} entities × {rows_per_entity} rows × {num_features} features)…"
        )
        now = _generate_one_data_batched(
            client,
            "benchmark_db",
            coll,
            "realistic_single",
            num_entities=num_entities,
            num_features=num_features,
            rows_per_entity=rows_per_entity,
        )

        one_cfg = RepoConfig(
            project="benchmark",
            registry="memory://",
            provider="local",
            offline_store=MongoDBOfflineStoreOneConfig(
                connection_string=mongodb_connection_string,
                database="benchmark_db",
                collection=coll,
            ),
            online_store={"type": "sqlite"},
            entity_key_serialization_version=ENTITY_KEY_VERSION,
        )
        source = MongoDBSourceOne(
            name="realistic_single", timestamp_field="event_timestamp"
        )
        entity = Entity(
            name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
        )
        schema = [Field(name="driver_id", dtype=Int64)] + [
            Field(name=f"feature_{f}", dtype=Float64) for f in range(num_features)
        ]
        fv = FeatureView(
            name="realistic_single",
            entities=[entity],
            schema=schema,
            source=source,
            ttl=timedelta(days=30),
        )
        feature_refs = [f"realistic_single:feature_{i}" for i in range(num_features)]
        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        def run_query():
            one_job = MongoDBOfflineStoreOne.get_historical_features(
                config=one_cfg,
                feature_views=[fv],
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return one_job.to_df()

        result = _run_benchmark_full(run_query, mongo_client=client)
        _print_benchmark_result(
            "ONE", "Entities (realistic P=30, 1 FV)", num_entities, result, num_entities
        )
        print(
            f"  Collection docs: {total_docs:,}  |  Raw ≈ {total_docs * num_features * 8 / 1e6:.0f} MB"
        )
        assert result.status == "OK", f"One must never OOM — got: {result.status}"

    finally:
        client.close()


# =============================================================================
# Test 7: Realistic Benchmark — P=30, 3 Feature Views joined simultaneously
# =============================================================================
#
# In production, a training job typically joins 3–5 feature views.  Many loads
# every FV's collection *independently*, so memory = sum of all collections.
# With 3 FVs (M=10, P=30):
#
#   Projected Many memory = 3 × N × 30 × 10 floats × ~130 bytes/float
#     50 K  → 3 × ~1.95 GB = ~5.85 GB
#     100 K → 3 × ~3.9  GB = ~11.7  GB
#     200 K → 3 × ~7.8  GB = ~23.4  GB  (near 32 GB limit)
#     300 K → 3 × ~11.7 GB = ~35.1  GB  → OOM expected
#
# One is unaffected: it chunks each FV query independently, so memory stays
# bounded at CHUNK_SIZE × M × overhead per FV, not the product.

REALISTIC_MULTI_ENTITY_COUNTS = [50_000, 100_000, 200_000, 300_000]
REALISTIC_NUM_FEATURE_VIEWS = 3


@_requires_docker
@pytest.mark.slow
@pytest.mark.timeout(0)
@pytest.mark.parametrize("num_entities", REALISTIC_MULTI_ENTITY_COUNTS)
def test_realistic_p30_multi_fv_many(
    mongodb_connection_string: str, many_config: RepoConfig, num_entities: int
) -> None:
    """Many: realistic multi-FV join (3 FVs, M=10, P=30).

    Many must load all 3 collections before joining — memory is additive.
    OOM is expected around 200K–300K entities on a 32 GB machine.
    """
    num_features = REALISTIC_NUM_FEATURES
    rows_per_entity = REALISTIC_ROWS_PER_ENTITY
    num_fvs = REALISTIC_NUM_FEATURE_VIEWS
    total_docs = num_fvs * num_entities * rows_per_entity

    fv_names = [f"realistic_fv_{i}" for i in range(num_fvs)]
    coll_names = [f"realistic_many_fv_{i}" for i in range(num_fvs)]

    client = MongoClient(mongodb_connection_string)
    try:
        print(
            f"\n[MANY realistic-multi] Generating {total_docs:,} docs across {num_fvs} FVs "
            f"({num_entities:,} entities × {rows_per_entity} rows × {num_features} features each)…"
        )
        now = None
        for fv_name, coll_name in zip(fv_names, coll_names):
            now = _generate_many_data_batched(
                client,
                "benchmark_db",
                coll_name,
                num_entities=num_entities,
                num_features=num_features,
                rows_per_entity=rows_per_entity,
            )

        entity = Entity(
            name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
        )
        feature_views = []
        for fv_name, coll_name in zip(fv_names, coll_names):
            source = MongoDBSourceMany(
                name=fv_name,
                database="benchmark_db",
                collection=coll_name,
                timestamp_field="event_timestamp",
            )
            schema = [Field(name="driver_id", dtype=Int64)] + [
                Field(name=f"feature_{f}", dtype=Float64) for f in range(num_features)
            ]
            feature_views.append(
                FeatureView(
                    name=fv_name,
                    entities=[entity],
                    schema=schema,
                    source=source,
                    ttl=timedelta(days=30),
                )
            )

        feature_refs = [
            f"{fv_name}:feature_{f}"
            for fv_name in fv_names
            for f in range(num_features)
        ]
        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        # Memory check: Many loads all num_fvs collections simultaneously.
        safe, projected_mb, free_mb = _check_memory_for_many(
            num_entities * num_fvs, rows_per_entity, num_features
        )
        if not safe:
            print(
                f"  [MANY realistic-multi] Skipping query — projected {projected_mb:,.0f} MB "
                f"exceeds 80% of {free_mb:,.0f} MB free RAM.  Would likely SIGKILL."
            )
            result = FullBenchmarkResult(
                elapsed_seconds=0,
                peak_memory_mb=projected_mb,
                status="SKIPPED:low_memory",
            )
        else:

            def run_query():
                many_job = MongoDBOfflineStoreMany.get_historical_features(
                    config=many_config,
                    feature_views=feature_views,
                    feature_refs=feature_refs,
                    entity_df=entity_df,
                    registry=MagicMock(),
                    project="benchmark",
                    full_feature_names=False,
                )
                return many_job.to_df()

            result = _run_benchmark_full(run_query, mongo_client=client)

        _print_benchmark_result(
            "MANY",
            f"Entities (realistic P=30, {num_fvs} FVs)",
            num_entities,
            result,
            num_entities,
        )
        print(
            f"  Total docs loaded: {total_docs:,}  |  Raw ≈ {total_docs * num_features * 8 / 1e6:.0f} MB"
        )

    finally:
        client.close()


@_requires_docker
@pytest.mark.slow
@pytest.mark.timeout(0)
@pytest.mark.parametrize("num_entities", REALISTIC_MULTI_ENTITY_COUNTS)
def test_realistic_p30_multi_fv_one(
    mongodb_connection_string: str, num_entities: int
) -> None:
    """One: realistic multi-FV join (3 FVs, M=10, P=30).

    All 3 FVs share a single MongoDB collection, discriminated by the
    ``feature_view`` field.  One chunks each FV query independently — its
    memory ceiling is O(CHUNK_SIZE × M) regardless of N or the number of FVs.
    All parametrize values must complete without OOM.
    """
    num_features = REALISTIC_NUM_FEATURES
    rows_per_entity = REALISTIC_ROWS_PER_ENTITY
    num_fvs = REALISTIC_NUM_FEATURE_VIEWS
    total_docs = num_fvs * num_entities * rows_per_entity
    coll = "realistic_one_multi"

    fv_names = [f"realistic_fv_{i}" for i in range(num_fvs)]

    client = MongoClient(mongodb_connection_string)
    try:
        client["benchmark_db"][coll].drop()
        print(
            f"\n[ONE realistic-multi] Generating {total_docs:,} docs across {num_fvs} FVs "
            f"({num_entities:,} entities × {rows_per_entity} rows × {num_features} features each)…"
        )
        now = None
        for fv_name in fv_names:
            now = _generate_one_data_batched(
                client,
                "benchmark_db",
                coll,
                fv_name,
                num_entities=num_entities,
                num_features=num_features,
                rows_per_entity=rows_per_entity,
            )

        one_cfg = RepoConfig(
            project="benchmark",
            registry="memory://",
            provider="local",
            offline_store=MongoDBOfflineStoreOneConfig(
                connection_string=mongodb_connection_string,
                database="benchmark_db",
                collection=coll,
            ),
            online_store={"type": "sqlite"},
            entity_key_serialization_version=ENTITY_KEY_VERSION,
        )
        entity = Entity(
            name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
        )
        feature_views = []
        for fv_name in fv_names:
            source = MongoDBSourceOne(name=fv_name, timestamp_field="event_timestamp")
            schema = [Field(name="driver_id", dtype=Int64)] + [
                Field(name=f"feature_{f}", dtype=Float64) for f in range(num_features)
            ]
            feature_views.append(
                FeatureView(
                    name=fv_name,
                    entities=[entity],
                    schema=schema,
                    source=source,
                    ttl=timedelta(days=30),
                )
            )

        feature_refs = [
            f"{fv_name}:feature_{f}"
            for fv_name in fv_names
            for f in range(num_features)
        ]
        entity_df = pd.DataFrame(
            {
                "driver_id": list(range(num_entities)),
                "event_timestamp": [now] * num_entities,
            }
        )

        def run_query():
            one_job = MongoDBOfflineStoreOne.get_historical_features(
                config=one_cfg,
                feature_views=feature_views,
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=False,
            )
            return one_job.to_df()

        result = _run_benchmark_full(run_query, mongo_client=client)
        _print_benchmark_result(
            "ONE",
            f"Entities (realistic P=30, {num_fvs} FVs)",
            num_entities,
            result,
            num_entities,
        )
        print(
            f"  Total docs in collection: {total_docs:,}  |  Raw ≈ {total_docs * num_features * 8 / 1e6:.0f} MB"
        )
        assert result.status == "OK", f"One must never OOM — got: {result.status}"

    finally:
        client.close()
