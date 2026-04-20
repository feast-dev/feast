"""MongoDB offline store benchmark suite — v2.

Sweeps four independent dimensions across all three implementations
side-by-side in every parametrize run:

  N  entity count          (entity_df rows)
  M  feature width         (features per feature view)
  P  observation depth     (historical rows per entity in the collection)
  K  feature view fan-out  (feature views joined simultaneously)

Implementations
---------------
  one    — single collection, Python-side PIT join (pandas merge_asof)
  native — single collection, Atlas-side PIT join ($documents + $lookup)
  many   — per-FV collections, Ibis-based PIT join

P is the key differentiator: native's $lookup subpipeline uses
``{$sort: {event_timestamp: -1}, $limit: 1}`` backed by the compound index,
so its cost is O(log P) per lookup.  one and many must transfer all N×P
documents from MongoDB and deduplicate in Python — cost is O(N×P).

Output
------
  stdout   side-by-side table after each parametrize run
  CSV      benchmark_results.csv in this directory (appended, never overwritten)

Usage
-----
  Smoke (default, ~2–3 min):
      pytest benchmark_v2.py -v -s

  Skip slow stress tests:
      pytest benchmark_v2.py -v -s -m "not slow"

  Stress only:
      pytest benchmark_v2.py -v -s -m slow

  Full sweep — edit the *_FULL constants below and swap them into the
  @pytest.mark.parametrize decorators:
      pytest benchmark_v2.py -v -s
"""

from __future__ import annotations

import csv
import gc
import resource
import sys
import time
import tracemalloc
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Set

import pandas as pd
import pytest
import pytz

pytest.importorskip("pymongo")

from unittest.mock import MagicMock

from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer

from feast import Entity, FeatureView, Field
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_agg import (
    MongoDBOfflineStoreAgg,
    MongoDBOfflineStoreAggConfig,
    MongoDBSourceAgg,
)
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_many import (
    MongoDBOfflineStoreMany,
    MongoDBOfflineStoreManyConfig,
    MongoDBSourceMany,
)
from feast.infra.offline_stores.contrib.mongodb_offline_store.mongodb_native import (
    MongoDBOfflineStoreNative,
    MongoDBOfflineStoreNativeConfig,
    MongoDBSourceNative,
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

# ---------------------------------------------------------------------------
# Docker guard
# ---------------------------------------------------------------------------

docker_available = False
try:
    import docker

    try:
        _docker_client = docker.from_env()
        _docker_client.ping()
        docker_available = True
    except Exception:
        pass
except ImportError:
    pass

_requires_docker = pytest.mark.skipif(
    not docker_available,
    reason="Docker is not available or not running.",
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ENTITY_KEY_VERSION = 3
BENCH_DB = "bench_db"
BENCH_FH = "bench_fh"  # single-collection name shared by 'one' and 'native'
_INSERT_BATCH = 50_000  # docs per insert_many call

_CSV_PATH = Path(__file__).with_name("benchmark_results.csv")

# ---------------------------------------------------------------------------
# Sweep ranges
# ---------------------------------------------------------------------------
#
# Smoke tier (default) — moderate sizes, safe for the Docker testcontainer.
# Each sweep holds the other three dimensions at their baseline.

N_BASE, M_BASE, P_BASE, K_BASE = 500, 10, 5, 1

N_SMOKE = [200, 1_000, 4_000]  # entities in entity_df
M_SMOKE = [5, 20, 50]  # features per feature view
P_SMOKE = [1, 5, 20]  # historical rows per entity
K_SMOKE = [1, 3]  # feature views joined

# 2×2 N×P interaction grid — reveals P-independence of 'native'
N_P_GRID_SMOKE = [(500, 5), (500, 20), (2_000, 5), (2_000, 20)]

# Full tier — swap into the @pytest.mark.parametrize decorators for local runs.
# N_FULL = [200, 1_000, 5_000, 20_000, 100_000]
# M_FULL = [5, 20, 50, 100, 200]
# P_FULL = [1, 5, 20, 60, 120]
# K_FULL = [1, 3, 7, 15]
# N_P_GRID_FULL = [(1_000,5),(1_000,60),(10_000,5),(10_000,60)]

# Stress tier — @pytest.mark.slow, large N.
N_STRESS = [50_000, 200_000]
M_STRESS, P_STRESS = 50, 5

# ---------------------------------------------------------------------------
# Entity key helper (int entity_id → serialized bytes)
# ---------------------------------------------------------------------------


def _make_entity_id(entity_id: int) -> bytes:
    entity_key = EntityKeyProto()
    entity_key.join_keys.append("driver_id")
    val = ValueProto()
    val.int64_val = entity_id
    entity_key.entity_values.append(val)
    return serialize_entity_key(entity_key, ENTITY_KEY_VERSION)


# ---------------------------------------------------------------------------
# Store implementation descriptors
# ---------------------------------------------------------------------------


@dataclass
class StoreImpl:
    """Bundles all implementation-specific factories into one object."""

    id: str
    store_class: Any
    make_offline_config: Callable[[str], Any]  # (conn_str) → offline store config
    make_source: Callable[[str], Any]  # (fv_name) → data source


ALL_IMPLS: List[StoreImpl] = [
    StoreImpl(
        id="one",
        store_class=MongoDBOfflineStoreOne,
        make_offline_config=lambda conn: MongoDBOfflineStoreOneConfig(
            connection_string=conn,
            database=BENCH_DB,
            collection=BENCH_FH,
        ),
        make_source=lambda fv_name: MongoDBSourceOne(
            name=fv_name,
            timestamp_field="event_timestamp",
            created_timestamp_column="created_at",
        ),
    ),
    StoreImpl(
        id="native",
        store_class=MongoDBOfflineStoreNative,
        make_offline_config=lambda conn: MongoDBOfflineStoreNativeConfig(
            connection_string=conn,
            database=BENCH_DB,
            collection=BENCH_FH,
        ),
        make_source=lambda fv_name: MongoDBSourceNative(
            name=fv_name,
            timestamp_field="event_timestamp",
            created_timestamp_column="created_at",
        ),
    ),
    StoreImpl(
        id="many",
        store_class=MongoDBOfflineStoreMany,
        make_offline_config=lambda conn: MongoDBOfflineStoreManyConfig(
            connection_string=conn,
            database=BENCH_DB,
        ),
        make_source=lambda fv_name: MongoDBSourceMany(
            name=fv_name,
            database=BENCH_DB,
            collection=fv_name,
            timestamp_field="event_timestamp",
        ),
    ),
    StoreImpl(
        id="agg",
        store_class=MongoDBOfflineStoreAgg,
        make_offline_config=lambda conn: MongoDBOfflineStoreAggConfig(
            connection_string=conn,
            database=BENCH_DB,
            collection=BENCH_FH,
        ),
        make_source=lambda fv_name: MongoDBSourceAgg(
            name=fv_name,
            timestamp_field="event_timestamp",
            created_timestamp_column="created_at",
        ),
    ),
]

# ---------------------------------------------------------------------------
# Seed helper — writes both schemas simultaneously
# ---------------------------------------------------------------------------


def _seed_benchmark(
    client: MongoClient,
    fv_names: List[str],
    num_entities: int,
    num_features: int,
    rows_per_entity: int,
) -> datetime:
    """Insert data in both the single-collection and per-FV schemas.

    Single-collection (BENCH_FH) serves 'one' and 'native'.
    Per-FV collections (one per fv_name) serve 'many'.

    Returns the ``now`` timestamp (the most recent event_timestamp seeded).
    """
    import numpy as np

    db = client[BENCH_DB]
    now = datetime.now(tz=pytz.UTC)
    # P timestamps: now, now-1h, now-2h, …, now-(P-1)h
    timestamps = [now - timedelta(hours=p) for p in range(rows_per_entity)]
    feat_names = [f"feature_{f}" for f in range(num_features)]

    for fv_idx, fv_name in enumerate(fv_names):
        rng = np.random.default_rng(seed=fv_idx)
        feat_matrix = rng.random((num_entities * rows_per_entity, num_features)).astype(
            float
        )

        single_batch: List[Dict] = []
        many_batch: List[Dict] = []
        row_idx = 0

        for entity_id in range(num_entities):
            eid_bytes = _make_entity_id(entity_id)
            for p in range(rows_per_entity):
                ts = timestamps[p]
                feat_vals = {
                    feat_names[f]: float(feat_matrix[row_idx, f])
                    for f in range(num_features)
                }
                row_idx += 1

                # Single-collection schema (one + native)
                single_batch.append(
                    {
                        "entity_id": eid_bytes,
                        "feature_view": fv_name,
                        "features": feat_vals,
                        "event_timestamp": ts,
                        "created_at": ts,
                    }
                )
                # Per-FV schema (many)
                many_batch.append(
                    {"driver_id": entity_id, **feat_vals, "event_timestamp": ts}
                )

                if len(single_batch) >= _INSERT_BATCH:
                    db[BENCH_FH].insert_many(single_batch)
                    db[fv_name].insert_many(many_batch)
                    single_batch = []
                    many_batch = []

        if single_batch:
            db[BENCH_FH].insert_many(single_batch)
            db[fv_name].insert_many(many_batch)

    return now


_COMPOUND_IDX = [
    ("entity_id", 1),
    ("feature_view", 1),
    ("event_timestamp", -1),
    ("created_at", -1),
]


def _reset_and_seed(
    client: MongoClient,
    fv_names: List[str],
    num_entities: int,
    num_features: int,
    rows_per_entity: int,
) -> datetime:
    """Drop benchmark collections, seed fresh data, then build index.

    The compound index is created AFTER seeding so that the bulk insert is
    not slowed by incremental index maintenance.  All four implementations
    share this index; building it once here means the first benchmark query
    never has to wait for a concurrent background index build.
    """
    db = client[BENCH_DB]
    db[BENCH_FH].drop()
    for fv_name in fv_names:
        db[fv_name].drop()
    now = _seed_benchmark(client, fv_names, num_entities, num_features, rows_per_entity)
    # Build the compound index synchronously so queries are never forced to scan.
    db[BENCH_FH].create_index(_COMPOUND_IDX, name="entity_fv_ts_idx")
    return now


# ---------------------------------------------------------------------------
# Measurement harness
# ---------------------------------------------------------------------------


def _rss_mb() -> float:
    """Current process peak RSS in MB (lifetime high-water-mark from the OS).

    We snapshot before/after each operation; the delta reflects the high-water
    added by that operation.  On macOS ru_maxrss is in bytes; on Linux, KB.
    """
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / (1024 * 1024) if sys.platform == "darwin" else rss / 1024


@dataclass
class BenchResult:
    elapsed_s: float
    trace_mb: float  # tracemalloc Python-heap peak for this operation
    rss_mb: float = 0.0  # RSS growth attributed to this operation
    mongo_ops: Dict[str, int] = field(default_factory=dict)
    status: str = "OK"  # "OK" | "OOM" | "SKIPPED:…" | "ERROR:…"


def _run_measured(func: Callable, mongo_client: Optional[Any] = None) -> BenchResult:
    """Run func() and capture elapsed time, tracemalloc peak, RSS delta, Mongo ops."""
    # MongoDB opcounters snapshot
    mongo_before: Optional[Dict] = None
    if mongo_client:
        try:
            status = mongo_client.admin.command("serverStatus")
            mongo_before = dict(status.get("opcounters", {}))
        except Exception:
            pass

    rss_before = _rss_mb()
    tracemalloc.start()
    t0 = time.perf_counter()

    outcome = "OK"
    try:
        func()
    except MemoryError:
        outcome = "OOM"
    except Exception as exc:
        outcome = f"ERROR:{str(exc)[:80]}"

    elapsed = time.perf_counter() - t0
    _, peak_bytes = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    rss_after = _rss_mb()

    mongo_delta: Dict[str, int] = {}
    if mongo_client and mongo_before:
        try:
            after = mongo_client.admin.command("serverStatus")
            after_ops = dict(after.get("opcounters", {}))
            mongo_delta = {
                k: after_ops.get(k, 0) - mongo_before.get(k, 0) for k in after_ops
            }
        except Exception:
            pass

    return BenchResult(
        elapsed_s=elapsed,
        trace_mb=peak_bytes / (1024 * 1024),
        rss_mb=max(rss_after - rss_before, 0.0),
        mongo_ops=mongo_delta,
        status=outcome,
    )


# ---------------------------------------------------------------------------
# OOM projection utilities (carried over from benchmark.py)
# ---------------------------------------------------------------------------

# Empirical overhead: bytes consumed by 'many' per raw float value.
# Measured in stress runs: tracemalloc peak / (N × P × M floats).
_MANY_BYTES_PER_FLOAT = 130


def _projected_many_mb(N: int, P: int, M: int) -> float:
    return N * P * M * _MANY_BYTES_PER_FLOAT / 1e6


def _free_memory_mb() -> float:
    """Estimate currently available RAM in MB (macOS vm_stat or /proc/meminfo)."""
    try:
        import subprocess

        out = subprocess.check_output(["vm_stat"], text=True)
        page_size = 16_384
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
    return 4_096  # conservative fallback


def _many_safe(N: int, P: int, M: int, safety: float = 0.80) -> tuple:
    """Return (safe, projected_mb, free_mb)."""
    proj = _projected_many_mb(N, P, M)
    free = _free_memory_mb()
    return proj < free * safety, proj, free


# ---------------------------------------------------------------------------
# FV / config factories
# ---------------------------------------------------------------------------


def _make_fv(impl: StoreImpl, fv_name: str, num_features: int) -> FeatureView:
    entity = Entity(
        name="driver_id", join_keys=["driver_id"], value_type=ValueType.INT64
    )
    schema = [Field(name="driver_id", dtype=Int64)] + [
        Field(name=f"feature_{f}", dtype=Float64) for f in range(num_features)
    ]
    return FeatureView(
        name=fv_name,
        entities=[entity],
        schema=schema,
        source=impl.make_source(fv_name),
        ttl=timedelta(days=100),  # generous — never filters during benchmark
    )


def _make_config(impl: StoreImpl, conn_str: str) -> RepoConfig:
    return RepoConfig(
        project="benchmark",
        registry="memory://",
        provider="local",
        offline_store=impl.make_offline_config(conn_str),
        online_store={"type": "sqlite"},
        entity_key_serialization_version=ENTITY_KEY_VERSION,
    )


# ---------------------------------------------------------------------------
# Run all three implementations for one scenario
# ---------------------------------------------------------------------------


def _run_all(
    conn_str: str,
    fv_names: List[str],
    num_features: int,
    entity_df: pd.DataFrame,
    mongo_client: Any,
    skip_many_if_oom: bool = False,
    skip_impl_ids: Optional[Set[str]] = None,
    N: int = 0,
    P: int = 0,
) -> Dict[str, BenchResult]:
    """Run get_historical_features on every impl and return per-impl BenchResult."""
    feature_refs = [f"{n}:feature_{f}" for n in fv_names for f in range(num_features)]
    results: Dict[str, BenchResult] = {}

    for impl in ALL_IMPLS:
        # Caller-supplied skip list (e.g. native at high K due to COLLSCAN × K issue)
        if skip_impl_ids and impl.id in skip_impl_ids:
            results[impl.id] = BenchResult(
                elapsed_s=0,
                trace_mb=0,
                status="SKIPPED:excluded",
            )
            continue

        # OOM guard for 'many' in stress scenarios.
        # 'many' processes K feature views sequentially but does NOT release
        # the merged result between iterations — the full K×N×M array
        # accumulates.  Multiply the per-FV projection by K before checking.
        if skip_many_if_oom and impl.id == "many" and N > 0 and P > 0:
            K = len(fv_names)
            per_fv_mb = _projected_many_mb(N, P, num_features)
            total_proj_mb = per_fv_mb * K
            free_mb = _free_memory_mb()
            safe = total_proj_mb < free_mb * 0.80
            proj_mb = total_proj_mb
            if not safe:
                results[impl.id] = BenchResult(
                    elapsed_s=0,
                    trace_mb=proj_mb,
                    status=f"SKIPPED:{proj_mb:.0f}MB_projected",
                )
                print(
                    f"  [many] SKIPPED — projected {proj_mb:.0f} MB "
                    f"(K={K} × {per_fv_mb:.0f} MB/FV) > 80 % of {free_mb:.0f} MB free"
                )
                continue

        gc.collect()  # release residual memory from previous impl

        config = _make_config(impl, conn_str)
        fvs = [_make_fv(impl, n, num_features) for n in fv_names]

        def _query(impl=impl, config=config, fvs=fvs):
            job = impl.store_class.get_historical_features(
                config=config,
                feature_views=fvs,
                feature_refs=feature_refs,
                entity_df=entity_df,
                registry=MagicMock(),
                project="benchmark",
                full_feature_names=True,
            )
            return job.to_df()

        results[impl.id] = _run_measured(_query, mongo_client=mongo_client)

    return results


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

_COL_W = 13  # column width per implementation in the table


def _print_table(test: str, params: Dict, results: Dict[str, BenchResult]) -> None:
    """Print a side-by-side results table to stdout."""
    impl_ids = list(results.keys())
    N = params.get("N", 0)

    title = (
        f"[{test}]  N={params.get('N', '?')}  "
        f"M={params.get('M', '?')}  "
        f"P={params.get('P', '?')}  "
        f"K={params.get('K', '?')}"
    )
    bar = "─" * (22 + _COL_W * len(impl_ids))
    print(f"\n{title}")
    print(f"  {'':22}" + "".join(f"{i:>{_COL_W}}" for i in impl_ids))
    print(f"  {bar}")

    def _cell(r: Optional[BenchResult], metric: str) -> str:
        if r is None:
            return "—"
        if r.status != "OK" and metric != "status":
            return r.status[: _COL_W - 1]
        if metric == "time_s":
            return f"{r.elapsed_s:.3f}"
        if metric == "trace_mb":
            return f"{r.trace_mb:.1f}"
        if metric == "rss_mb":
            return f"{r.rss_mb:.1f}"
        if metric == "rows_s":
            return f"{N / r.elapsed_s:,.0f}" if r.elapsed_s > 0 and N > 0 else "—"
        if metric == "status":
            return r.status
        return "—"

    for metric, label in [
        ("time_s", "time (s)"),
        ("trace_mb", "trace MB"),
        ("rss_mb", "RSS Δ MB"),
        ("rows_s", "rows/s"),
        ("status", "status"),
    ]:
        row = f"  {label:22}" + "".join(
            f"{_cell(results.get(iid), metric):>{_COL_W}}" for iid in impl_ids
        )
        print(row)


def _append_csv(test: str, params: Dict, results: Dict[str, BenchResult]) -> None:
    """Append one row per implementation to benchmark_results.csv."""
    from datetime import datetime as _dt

    ts = _dt.now().isoformat(timespec="seconds")
    N = params.get("N", 0)
    fieldnames = [
        "timestamp",
        "test",
        "impl",
        "N",
        "M",
        "P",
        "K",
        "elapsed_s",
        "trace_mb",
        "rss_mb",
        "rows_per_s",
        "status",
    ]
    rows = [
        {
            "timestamp": ts,
            "test": test,
            "impl": impl_id,
            "N": params.get("N", ""),
            "M": params.get("M", ""),
            "P": params.get("P", ""),
            "K": params.get("K", ""),
            "elapsed_s": round(r.elapsed_s, 4),
            "trace_mb": round(r.trace_mb, 2),
            "rss_mb": round(r.rss_mb, 2),
            "rows_per_s": round(N / r.elapsed_s, 1) if r.elapsed_s > 0 and N > 0 else 0,
            "status": r.status,
        }
        for impl_id, r in results.items()
    ]
    write_header = not _CSV_PATH.exists()
    with _CSV_PATH.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mongodb_container() -> Generator[Optional[MongoDbContainer], None, None]:
    import os

    if os.environ.get("MONGODB_URI"):
        yield None  # external cluster — no container needed
        return
    container = MongoDbContainer(
        "mongo:latest",
        username="test",
        password="test",  # pragma: allowlist secret
    ).with_exposed_ports(27017)
    container.start()
    yield container
    container.stop()


@pytest.fixture
def conn_str(mongodb_container: Optional[MongoDbContainer]) -> str:
    import os

    uri = os.environ.get("MONGODB_URI")
    if uri:
        return uri
    assert mongodb_container is not None
    port = mongodb_container.get_exposed_port(27017)
    return f"mongodb://test:test@localhost:{port}"  # pragma: allowlist secret


# =============================================================================
# Sweep 1: N — entity count
#
# What to look for:
#   one, many : time and memory grow roughly linearly with N (they load N×P docs)
#   native    : time grows linearly with N (one $lookup per entity row),
#               memory grows linearly with N (result is N×M features)
#               but the per-entity cost should be lower than one/many because
#               the PIT deduplication happens inside the $lookup on the server.
# =============================================================================


@_requires_docker
@pytest.mark.parametrize("N", N_SMOKE)
def test_scale_N(conn_str: str, N: int) -> None:
    """Sweep N (entities). Hold M=M_BASE, P=P_BASE, K=K_BASE."""
    M, P, K = M_BASE, P_BASE, K_BASE
    fv_names = [f"fv_{k}" for k in range(K)]
    params = {"N": N, "M": M, "P": P, "K": K}

    client = MongoClient(conn_str)
    try:
        now = _reset_and_seed(client, fv_names, N, M, P)
        entity_df = pd.DataFrame(
            {"driver_id": list(range(N)), "event_timestamp": [now] * N}
        )
        results = _run_all(conn_str, fv_names, M, entity_df, client)
    finally:
        client.close()

    _print_table("scale_N", params, results)
    _append_csv("scale_N", params, results)


# =============================================================================
# Sweep 2: M — feature width
#
# What to look for:
#   native    : M affects result size only (one doc per entity row returned);
#               the $lookup pipeline itself is M-independent
#   one       : M affects how many keys are extracted from each features subdoc
#               (per-feature .apply()) — should scale gently with M
#   many      : M inflates every stored document; pandas DataFrame grows O(N×P×M)
# =============================================================================


@_requires_docker
@pytest.mark.parametrize("M", M_SMOKE)
def test_scale_M(conn_str: str, M: int) -> None:
    """Sweep M (features per FV). Hold N=N_BASE, P=P_BASE, K=K_BASE."""
    N, P, K = N_BASE, P_BASE, K_BASE
    fv_names = [f"fv_{k}" for k in range(K)]
    params = {"N": N, "M": M, "P": P, "K": K}

    client = MongoClient(conn_str)
    try:
        now = _reset_and_seed(client, fv_names, N, M, P)
        entity_df = pd.DataFrame(
            {"driver_id": list(range(N)), "event_timestamp": [now] * N}
        )
        results = _run_all(conn_str, fv_names, M, entity_df, client)
    finally:
        client.close()

    _print_table("scale_M", params, results)
    _append_csv("scale_M", params, results)


# =============================================================================
# Sweep 3: P — observation depth (historical rows per entity)
#
# This is the most diagnostic sweep for the native implementation.
#
# What to look for:
#   native : time and memory should be nearly flat as P grows — the $lookup
#            subpipeline finds the single matching document via the compound
#            index (entity_id, feature_view, event_timestamp DESC) and never
#            materialises the other P-1 rows.  Cost is O(log P) not O(P).
#   one    : fetches all N×P documents matching the entity_id $in list, then
#            does pandas merge_asof.  Both time and memory scale O(P).
#   many   : loads the entire collection (N×P docs) into pandas before joining.
#            Both time and memory scale O(P).
# =============================================================================


@_requires_docker
@pytest.mark.parametrize("P", P_SMOKE)
def test_scale_P(conn_str: str, P: int) -> None:
    """Sweep P (observations per entity). Hold N=N_BASE, M=M_BASE, K=K_BASE."""
    N, M, K = N_BASE, M_BASE, K_BASE
    fv_names = [f"fv_{k}" for k in range(K)]
    params = {"N": N, "M": M, "P": P, "K": K}

    client = MongoClient(conn_str)
    try:
        now = _reset_and_seed(client, fv_names, N, M, P)
        entity_df = pd.DataFrame(
            {"driver_id": list(range(N)), "event_timestamp": [now] * N}
        )
        results = _run_all(conn_str, fv_names, M, entity_df, client)
    finally:
        client.close()

    _print_table("scale_P", params, results)
    _append_csv("scale_P", params, results)


# =============================================================================
# Sweep 4: K — feature view fan-out
#
# What to look for:
#   native : each additional FV adds one $lookup stage to the pipeline;
#            the entire multi-FV join runs in a single aggregation round-trip.
#            Overhead per FV should be small.
#   one    : one aggregation query per FV (chunked), then one merge_asof pass
#            per FV.  Time grows roughly linearly with K.
#   many   : one full collection scan per FV, then one merge_asof per FV.
#            Both time and memory grow linearly with K.
# =============================================================================


@_requires_docker
@pytest.mark.parametrize("K", K_SMOKE)
def test_scale_K(conn_str: str, K: int) -> None:
    """Sweep K (feature views joined). Hold N=N_BASE, M=M_BASE, P=P_BASE."""
    N, M, P = N_BASE, M_BASE, P_BASE
    fv_names = [f"fv_{k}" for k in range(K)]
    params = {"N": N, "M": M, "P": P, "K": K}

    client = MongoClient(conn_str)
    try:
        now = _reset_and_seed(client, fv_names, N, M, P)
        entity_df = pd.DataFrame(
            {"driver_id": list(range(N)), "event_timestamp": [now] * N}
        )
        results = _run_all(conn_str, fv_names, M, entity_df, client)
    finally:
        client.close()

    _print_table("scale_K", params, results)
    _append_csv("scale_K", params, results)


# =============================================================================
# Interaction: N × P
#
# Varying N and P together reveals whether native's P-independence holds across
# entity scales.  If native is truly P-independent, (N=2000,P=20) should take
# roughly 4× longer than (N=500,P=20) — the same ratio as (N=2000,P=5) vs
# (N=500,P=5).  For one and many the P dimension multiplies the N cost.
#
# Expected pattern (if native's index is effective):
#   native  time(N=500,P=20)  ≈  time(N=500,P=5)      ← P doesn't matter
#   one     time(N=500,P=20)  ≈  4× time(N=500,P=5)   ← linear in P
#   many    time(N=500,P=20)  ≈  4× time(N=500,P=5)   ← linear in P
# =============================================================================


@_requires_docker
@pytest.mark.parametrize("N,P", N_P_GRID_SMOKE)
def test_interaction_N_P(conn_str: str, N: int, P: int) -> None:
    """2×2 N×P grid. Hold M=M_BASE, K=K_BASE."""
    M, K = M_BASE, K_BASE
    fv_names = [f"fv_{k}" for k in range(K)]
    params = {"N": N, "M": M, "P": P, "K": K}

    client = MongoClient(conn_str)
    try:
        now = _reset_and_seed(client, fv_names, N, M, P)
        entity_df = pd.DataFrame(
            {"driver_id": list(range(N)), "event_timestamp": [now] * N}
        )
        results = _run_all(conn_str, fv_names, M, entity_df, client)
    finally:
        client.close()

    _print_table("interaction_N_P", params, results)
    _append_csv("interaction_N_P", params, results)


# =============================================================================
# Stress: large N — memory scaling and OOM boundary
#
# At large N the memory difference between the implementations becomes stark:
#   many   loads N×P documents → O(N×P×M) bytes on the client.
#          A MemoryError (or SKIPPED if projected to SIGKILL) is expected.
#   one    fetches via CHUNK_SIZE-entity batches → O(CHUNK_SIZE×P×M) peak,
#          independent of total N.  Must always complete.
#   native fetches exactly 1 document per entity row → O(N×M) data returned,
#          but the server-side $lookup + index scan is O(N) not O(N×P).
#          At N=200k the $documents BSON payload (~10–15 MB) approaches
#          MongoDB's 16 MB message limit; very large N may require chunking.
# =============================================================================


@_requires_docker
@pytest.mark.slow
@pytest.mark.timeout(0)
@pytest.mark.parametrize("N", N_STRESS)
def test_stress_scale_N(conn_str: str, N: int) -> None:
    """Large-N stress test. all three implementations; OOM guard for 'many'."""
    M, P, K = M_STRESS, P_STRESS, K_BASE
    fv_names = [f"fv_{k}" for k in range(K)]
    params = {"N": N, "M": M, "P": P, "K": K}
    total_docs = N * P

    client = MongoClient(conn_str)
    try:
        print(
            f"\n[stress] Generating {total_docs:,} docs "
            f"({N:,} entities × {P} rows × {M} features)…"
        )
        now = _reset_and_seed(client, fv_names, N, M, P)
        entity_df = pd.DataFrame(
            {"driver_id": list(range(N)), "event_timestamp": [now] * N}
        )
        results = _run_all(
            conn_str,
            fv_names,
            M,
            entity_df,
            client,
            skip_many_if_oom=True,
            N=N,
            P=P,
        )
    finally:
        client.close()

    _print_table("stress_scale_N", params, results)
    _append_csv("stress_scale_N", params, results)
    print(f"  Total docs: {total_docs:,}  |  Raw ≈ {total_docs * M * 8 / 1e6:.0f} MB")

    assert results["one"].status == "OK", (
        f"'one' must complete at any scale via chunked fetching — got {results['one'].status}"
    )


# =============================================================================
# Stress: OOM crossover — all three on the same large dataset
#
# Runs all three implementations on a single fixed scale chosen to put 'many'
# well into OOM territory on a ≤32 GB machine while 'one' and 'native' finish.
#
# Scale: OOM_N × OOM_P × OOM_M total float values in MongoDB.
#
#   many    projected ≈ _MANY_BYTES_PER_FLOAT × OOM_N × OOM_P × OOM_M MB
#           → expected OOM or SKIPPED on ≤32 GB machines
#   one     CHUNK_SIZE-bounded → completes with ~several hundred MB
#   native  O(N×M) result → completes, but may be slow at OOM_N=200k
# =============================================================================

OOM_N = 200_000
OOM_M = 100
OOM_P = 5


@_requires_docker
@pytest.mark.slow
@pytest.mark.timeout(0)
def test_stress_oom_crossover(conn_str: str) -> None:
    """OOM crossover demonstration — 'many' OOMs where 'one' and 'native' succeed."""
    N, M, P, K = OOM_N, OOM_M, OOM_P, K_BASE
    fv_names = [f"fv_{k}" for k in range(K)]
    total_docs = N * P
    params = {"N": N, "M": M, "P": P, "K": K}

    print(
        f"\n{'=' * 72}\n"
        f"OOM CROSSOVER DEMO\n"
        f"  Scale  : {N:,} entities × {P} rows × {M} features\n"
        f"  Docs   : {total_docs:,}  |  raw ≈ {total_docs * M * 8 / 1e6:.0f} MB\n"
        f"  many projected peak ≈ {_projected_many_mb(N, P, M):,.0f} MB\n"
        f"{'=' * 72}"
    )

    client = MongoClient(conn_str)
    try:
        print(f"Generating {total_docs:,} docs…")
        now = _reset_and_seed(client, fv_names, N, M, P)
        entity_df = pd.DataFrame(
            {"driver_id": list(range(N)), "event_timestamp": [now] * N}
        )
        results = _run_all(
            conn_str,
            fv_names,
            M,
            entity_df,
            client,
            skip_many_if_oom=True,
            N=N,
            P=P,
        )
    finally:
        client.close()

    _print_table("stress_oom_crossover", params, results)
    _append_csv("stress_oom_crossover", params, results)

    assert results["one"].status == "OK", (
        f"'one' must complete regardless of scale — got {results['one'].status}"
    )


# =============================================================================
# Stress: K fan-out — K-collapse comparison
#
# All K feature views share the same driver_id join key.
#
#   agg    : K-collapses all K FVs into ONE $match+$sort+$group aggregation,
#            regardless of K.  Round trips = ceil(N / MONGO_BATCH_SIZE).
#   one    : issues K separate $match aggregations per batch.
#            Round trips = K × ceil(N / MONGO_BATCH_SIZE).
#   many   : does K full per-FV collection scans, one per FV.
#   native : SKIPPED at all K — it issues K independent $documents+$lookup
#            pipelines, each doing a full COLLSCAN of the shared collection
#            (COLLSCAN cost is O(K × N × total_docs) — impractical at K ≥ 10).
#
# Expected pattern:
#   agg    time ≈ constant as K grows (single aggregation, K-collapse)
#   one    time grows O(K)            (K round trips)
#   many   time grows O(K)            (K collection scans, large network transfer)
# =============================================================================

N_FAN_K = 2_000
M_FAN_K = 100
P_FAN_K = 5
K_FAN_VALUES = [1, 10, 100]


@_requires_docker
@pytest.mark.slow
@pytest.mark.timeout(0)
@pytest.mark.parametrize("K", K_FAN_VALUES)
def test_stress_fan_K(conn_str: str, K: int) -> None:
    """K fan-out stress test: measures K-collapse benefit of agg vs one/many.

    native is always skipped: it issues K independent $documents+$lookup pipelines
    each doing a COLLSCAN of the shared collection.  At K=10 with N=2000 (100 k docs)
    this is already impractical; at K=100 it would never complete.
    """
    N, M, P = N_FAN_K, M_FAN_K, P_FAN_K
    total_docs = N * P * K
    fv_names = [f"fv_{k}" for k in range(K)]
    params = {"N": N, "M": M, "P": P, "K": K}

    client = MongoClient(conn_str)
    try:
        print(
            f"\n[stress_fan_K] Seeding {total_docs:,} docs "
            f"({N:,} entities × {P} rows × {M} features × {K} FVs)…"
        )
        now = _reset_and_seed(client, fv_names, N, M, P)
        entity_df = pd.DataFrame(
            {"driver_id": list(range(N)), "event_timestamp": [now] * N}
        )
        results = _run_all(
            conn_str,
            fv_names,
            M,
            entity_df,
            client,
            skip_many_if_oom=True,
            skip_impl_ids={"native"},  # K COLLSCANs — impractical at K >= 10
            N=N,
            P=P,
        )
    finally:
        client.close()

    _print_table("stress_fan_K", params, results)
    _append_csv("stress_fan_K", params, results)
    print(f"  Total docs: {total_docs:,}  |  Raw ≈ {total_docs * M * 8 / 1e6:.0f} MB")

    assert results["agg"].status == "OK", (
        f"'agg' must complete — got {results['agg'].status}"
    )
