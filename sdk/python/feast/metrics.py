# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Centralized Prometheus metrics for the Feast feature server.

All metrics are defined here to provide a single source of truth.
Instrumentation is **opt-in**: metric recording is gated behind a
``_config`` object whose flags are only set when
``start_metrics_server()`` is called (i.e. when the feature server is
started with ``--metrics`` or ``metrics.enabled: true`` in the YAML).

Each metric category can be individually toggled via the ``metrics``
sub-block in ``feature_store.yaml``.  When disabled, helpers
short-circuit with a fast attribute check and do zero work.

Multiprocess support
--------------------
Gunicorn pre-forks worker processes, so every worker gets its own copy
of the in-process metric state.  To aggregate across workers we use
``prometheus_client``'s multiprocess mode:

1. ``PROMETHEUS_MULTIPROCESS_DIR`` is set (to a temp dir if the user
   has not already set it) **before** any metric objects are created.
2. Gauges specify ``multiprocess_mode`` so they aggregate correctly.
3. The metrics HTTP server uses ``MultiProcessCollector`` to read all
   workers' metric files.
4. Gunicorn hooks (``post_worker_init``, ``child_exit``) are wired up
   in ``feature_server.py`` to start per-worker monitoring and to
   clean up dead-worker files.
"""

import atexit
import logging
import os
import shutil
import tempfile
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

import psutil

if TYPE_CHECKING:
    from feast.feature_store import FeatureStore

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Multiprocess directory setup — MUST happen before prometheus_client import
# so that metric values are stored in shared files rather than in-process
# memory (required for Gunicorn pre-fork workers).
# ---------------------------------------------------------------------------
_prometheus_mp_dir: Optional[str] = None
_owns_mp_dir: bool = False
_owner_pid: Optional[int] = None

if "PROMETHEUS_MULTIPROCESS_DIR" not in os.environ:
    _prometheus_mp_dir = tempfile.mkdtemp(prefix="feast_metrics_")
    os.environ["PROMETHEUS_MULTIPROCESS_DIR"] = _prometheus_mp_dir
    _owns_mp_dir = True
    _owner_pid = os.getpid()
else:
    _prometheus_mp_dir = os.environ["PROMETHEUS_MULTIPROCESS_DIR"]

# prometheus_client uses two different env var names:
#   - PROMETHEUS_MULTIPROCESS_DIR  (for value storage in prometheus_client.values)
#   - PROMETHEUS_MULTIPROC_DIR     (for MultiProcessCollector)
# Both must point to the same directory.
if "PROMETHEUS_MULTIPROC_DIR" not in os.environ:
    os.environ["PROMETHEUS_MULTIPROC_DIR"] = _prometheus_mp_dir


def _cleanup_multiprocess_dir():
    # Only the process that created the directory may remove it.
    # Forked Gunicorn workers inherit _owns_mp_dir=True but have a
    # different PID; letting them delete the shared directory would
    # break metrics for sibling workers and the metrics HTTP server.
    if (
        _owns_mp_dir
        and _owner_pid == os.getpid()
        and _prometheus_mp_dir
        and os.path.isdir(_prometheus_mp_dir)
    ):
        shutil.rmtree(_prometheus_mp_dir, ignore_errors=True)


atexit.register(_cleanup_multiprocess_dir)

# Now safe to import prometheus_client — it will detect the env var.
from prometheus_client import Counter, Gauge, Histogram  # noqa: E402


# ---------------------------------------------------------------------------
# Per-category runtime flags
# ---------------------------------------------------------------------------
@dataclass
class _MetricsFlags:
    """Runtime toggle for each metric category.

    All flags default to ``False`` (disabled).  ``start_metrics_server``
    flips them on according to the user's ``MetricsConfig``.
    """

    enabled: bool = False
    resource: bool = False
    request: bool = False
    online_features: bool = False
    push: bool = False
    materialization: bool = False
    freshness: bool = False


_config = _MetricsFlags()


def build_metrics_flags(metrics_config: Optional[object] = None) -> _MetricsFlags:
    """Build ``_MetricsFlags`` from a ``MetricsConfig`` object.

    If *metrics_config* is ``None`` (e.g. metrics activated purely via
    ``--metrics`` CLI with no YAML block), every category defaults to
    enabled.  Otherwise the per-category booleans are respected.
    """
    if metrics_config is None:
        return _MetricsFlags(
            enabled=True,
            resource=True,
            request=True,
            online_features=True,
            push=True,
            materialization=True,
            freshness=True,
        )
    return _MetricsFlags(
        enabled=True,
        resource=getattr(metrics_config, "resource", True),
        request=getattr(metrics_config, "request", True),
        online_features=getattr(metrics_config, "online_features", True),
        push=getattr(metrics_config, "push", True),
        materialization=getattr(metrics_config, "materialization", True),
        freshness=getattr(metrics_config, "freshness", True),
    )


# ---------------------------------------------------------------------------
# Resource metrics — multiprocess_mode="liveall" so each live worker
# reports its own CPU/memory with a ``pid`` label.
# ---------------------------------------------------------------------------
cpu_usage_gauge = Gauge(
    "feast_feature_server_cpu_usage",
    "CPU usage percentage of the Feast feature server process",
    multiprocess_mode="liveall",
)
memory_usage_gauge = Gauge(
    "feast_feature_server_memory_usage",
    "Memory usage percentage of the Feast feature server process",
    multiprocess_mode="liveall",
)

# ---------------------------------------------------------------------------
# HTTP request metrics (Counters & Histograms aggregate automatically)
# ---------------------------------------------------------------------------
request_count = Counter(
    "feast_feature_server_request_total",
    "Total number of requests to the Feast feature server",
    ["endpoint", "status"],
)
request_latency = Histogram(
    "feast_feature_server_request_latency_seconds",
    "Latency of requests to the Feast feature server in seconds",
    ["endpoint", "feature_count", "feature_view_count"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

# ---------------------------------------------------------------------------
# Online feature retrieval metrics
# ---------------------------------------------------------------------------
online_features_request_count = Counter(
    "feast_online_features_request_total",
    "Total online feature retrieval requests",
)
online_features_entity_count = Histogram(
    "feast_online_features_entity_count",
    "Number of entity rows per online feature request",
    buckets=(1, 5, 10, 25, 50, 100, 250, 500, 1000),
)

# ---------------------------------------------------------------------------
# Push / write metrics
# ---------------------------------------------------------------------------
push_request_count = Counter(
    "feast_push_request_total",
    "Total push requests to the feature store",
    ["push_source", "mode"],
)

# ---------------------------------------------------------------------------
# Materialization metrics
# ---------------------------------------------------------------------------
materialization_result_total = Counter(
    "feast_materialization_result_total",
    "Total materialization runs per feature view",
    ["feature_view", "status"],
)
materialization_duration_seconds = Histogram(
    "feast_materialization_duration_seconds",
    "Duration of materialization per feature view in seconds",
    ["feature_view"],
    buckets=(1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0),
)

# ---------------------------------------------------------------------------
# Sub-request timing — online store reads and ODFV transformations
# ---------------------------------------------------------------------------
online_store_read_duration_seconds = Histogram(
    "feast_feature_server_online_store_read_duration_seconds",
    "Duration of the online store read phase in seconds (covers all table reads including parallel async)",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
transformation_duration_seconds = Histogram(
    "feast_feature_server_transformation_duration_seconds",
    "Duration of on-demand feature view transformations on read in seconds",
    ["odfv_name", "mode"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
write_transformation_duration_seconds = Histogram(
    "feast_feature_server_write_transformation_duration_seconds",
    "Duration of on-demand feature view transformations on write in seconds",
    ["odfv_name", "mode"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# ---------------------------------------------------------------------------
# Feature freshness metrics — "max" shows the worst-case staleness across
# processes (freshness is identical regardless of which process computes it).
# ---------------------------------------------------------------------------
feature_freshness_seconds = Gauge(
    "feast_feature_freshness_seconds",
    "Seconds since the most recent materialization end time per feature view",
    ["feature_view", "project"],
    multiprocess_mode="max",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class RequestMetricsContext:
    """Mutable label holder yielded by :func:`track_request_latency`.

    Callers that need to resolve labels *inside* the ``with`` block
    (e.g. ``/get-online-features`` where the feature count is only
    known after ``_get_features`` succeeds) can set the attributes
    on the yielded object and they will be picked up in ``finally``.
    """

    __slots__ = ("feature_count", "feature_view_count")

    def __init__(self, feature_count: str = "", feature_view_count: str = ""):
        self.feature_count = feature_count
        self.feature_view_count = feature_view_count


@contextmanager
def track_request_latency(
    endpoint: str, feature_count: str = "", feature_view_count: str = ""
):
    """Context manager that records endpoint latency and increments request count.

    Yields a :class:`RequestMetricsContext` whose ``feature_count`` and
    ``feature_view_count`` attributes can be updated inside the block.
    The final values are used when recording the histogram and counter
    in ``finally``, so labels are accurate even when they depend on
    work done inside the block.

    Gated by the ``request`` category flag.
    """
    ctx = RequestMetricsContext(feature_count, feature_view_count)
    if not _config.request:
        yield ctx
        return

    start = time.monotonic()
    status_label = "success"
    try:
        yield ctx
    except Exception:
        status_label = "error"
        raise
    finally:
        elapsed = time.monotonic() - start
        request_latency.labels(
            endpoint=endpoint,
            feature_count=ctx.feature_count,
            feature_view_count=ctx.feature_view_count,
        ).observe(elapsed)
        request_count.labels(endpoint=endpoint, status=status_label).inc()


def track_online_features_entities(entity_count: int):
    """Record the number of entity rows in an online feature request."""
    if not _config.online_features:
        return
    online_features_request_count.inc()
    online_features_entity_count.observe(entity_count)


def track_push(push_source: str, mode: str):
    """Increment the push request counter."""
    if not _config.push:
        return
    push_request_count.labels(push_source=push_source, mode=mode).inc()


def track_online_store_read(duration_seconds: float):
    """Record the duration of the online store read phase."""
    if not _config.online_features:
        return
    online_store_read_duration_seconds.observe(duration_seconds)


def track_transformation(odfv_name: str, mode: str, duration_seconds: float):
    """Record the duration of an on-demand feature view read-path transformation."""
    if not _config.online_features:
        return
    transformation_duration_seconds.labels(odfv_name=odfv_name, mode=mode).observe(
        duration_seconds
    )


def track_write_transformation(odfv_name: str, mode: str, duration_seconds: float):
    """Record the duration of an on-demand feature view write-path transformation."""
    if not _config.online_features:
        return
    write_transformation_duration_seconds.labels(
        odfv_name=odfv_name, mode=mode
    ).observe(duration_seconds)


def track_materialization(
    feature_view_name: str, success: bool, duration_seconds: float
):
    """Record materialization outcome and duration for a single feature view."""
    if not _config.materialization:
        return
    status = "success" if success else "failure"
    materialization_result_total.labels(
        feature_view=feature_view_name, status=status
    ).inc()
    materialization_duration_seconds.labels(feature_view=feature_view_name).observe(
        duration_seconds
    )


def update_feature_freshness(
    store: "FeatureStore",
) -> None:
    """
    Compute and set the freshness gauge for every feature view in the registry.

    Freshness = now - most_recent_end_time (from materialization_intervals).
    A higher value means the feature data is more stale.
    """
    try:
        feature_views = store.list_feature_views(allow_cache=True)
        now = datetime.now(tz=timezone.utc)
        for fv in feature_views:
            end_time = fv.most_recent_end_time
            if end_time is not None:
                if end_time.tzinfo is None:
                    end_time = end_time.replace(tzinfo=timezone.utc)
                staleness = (now - end_time).total_seconds()
                feature_freshness_seconds.labels(
                    feature_view=fv.name, project=store.project
                ).set(staleness)
    except Exception:
        logger.debug("Failed to update feature freshness metrics", exc_info=True)


def monitor_resources(interval: int = 5):
    """Background thread target that updates CPU and memory usage gauges."""
    logger.debug("Starting resource monitoring with interval %d seconds", interval)
    p = psutil.Process()
    logger.debug("PID is %d", p.pid)
    while True:
        with p.oneshot():
            cpu_usage = p.cpu_percent()
            memory_usage = p.memory_percent()
            logger.debug("CPU usage: %s%%, Memory usage: %s%%", cpu_usage, memory_usage)
            cpu_usage_gauge.set(cpu_usage)
            memory_usage_gauge.set(memory_usage)
        time.sleep(interval)


def monitor_freshness(store: "FeatureStore", interval: int = 30):
    """Background thread target that periodically updates feature freshness gauges."""
    logger.debug(
        "Starting feature freshness monitoring with interval %d seconds", interval
    )
    while True:
        update_feature_freshness(store)
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Gunicorn multiprocess helpers
# ---------------------------------------------------------------------------


def mark_process_dead(pid: int):
    """Clean up metric files for a dead Gunicorn worker.

    Called from the Gunicorn ``child_exit`` hook so that stale worker
    data no longer appears in scraped output.
    """
    if not _config.enabled:
        return
    try:
        from prometheus_client import multiprocess

        multiprocess.mark_process_dead(pid)
    except Exception:
        logger.debug("Failed to mark process %d as dead", pid, exc_info=True)


def init_worker_monitoring():
    """Start resource monitoring inside a Gunicorn worker process.

    Called from the ``post_worker_init`` hook so that each worker
    tracks its own CPU/memory independently of the master.
    """
    if _config.resource:
        t = threading.Thread(target=monitor_resources, args=(5,), daemon=True)
        t.start()


def start_metrics_server(
    store: "FeatureStore",
    port: int = 8000,
    metrics_config: Optional["_MetricsFlags"] = None,
    start_resource_monitoring: bool = True,
):
    """
    Start the Prometheus metrics HTTP server and background monitoring threads.

    Uses ``MultiProcessCollector`` so that metrics from all Gunicorn
    workers are correctly aggregated when Prometheus scrapes port *port*.

    Args:
        store: The FeatureStore instance (used for freshness checks).
        port: TCP port for the Prometheus HTTP endpoint.
        metrics_config: Optional pre-built ``_MetricsFlags``.  When
            ``None`` every category defaults to **enabled**.
        start_resource_monitoring: Whether to start the CPU/memory
            monitoring thread.  Set to ``False`` when Gunicorn will
            fork workers — the ``post_worker_init`` hook starts
            per-worker monitoring instead.
    """
    global _config

    if metrics_config is not None:
        _config = metrics_config
    else:
        _config = _MetricsFlags(
            enabled=True,
            resource=True,
            request=True,
            online_features=True,
            push=True,
            materialization=True,
            freshness=True,
        )

    from prometheus_client import CollectorRegistry, make_wsgi_app
    from prometheus_client.multiprocess import MultiProcessCollector

    registry = CollectorRegistry()
    MultiProcessCollector(registry)

    from wsgiref.simple_server import make_server

    httpd = make_server("", port, make_wsgi_app(registry))
    metrics_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    metrics_thread.start()
    logger.info(
        "Prometheus metrics server started on port %d (multiprocess-safe)", port
    )

    if _config.resource and start_resource_monitoring:
        resource_thread = threading.Thread(
            target=monitor_resources, args=(5,), daemon=True
        )
        resource_thread.start()

    if _config.freshness:
        freshness_thread = threading.Thread(
            target=monitor_freshness, args=(store, 30), daemon=True
        )
        freshness_thread.start()
