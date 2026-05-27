"""
Feast tracing via MLflow Distributed Tracing.

Uses MLflow's native tracing API (``mlflow.start_span``) so that
server-side spans appear in the MLflow UI Traces tab and support
parent-child linking via ``traceparent`` headers (W3C TraceContext).

Initialization is lazy (post-fork safe for gunicorn): the first
request inside a worker calls ``mlflow.set_tracking_uri`` and
``mlflow.set_experiment``, then creates spans via ``mlflow.start_span``.
No background threads, no OTEL TracerProvider — everything goes
through MLflow's own transport to the configured tracking server.
"""

from __future__ import annotations

import contextlib
import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional

if TYPE_CHECKING:
    from feast.feature_store import FeatureStore

_logger = logging.getLogger(__name__)

_initialized = False
_enabled = False
_mlflow_mod: Any = None

_HAS_DISTRIBUTED_CTX = False


def _lazy_init(store: "FeatureStore") -> bool:
    """Initialize MLflow tracing on first use (post-fork safe).

    Only sets ``tracking_uri`` and ``experiment`` — no sockets, no
    threads, no gRPC channels.  ``mlflow.start_span`` lazily creates
    its internal OTEL machinery on the first span, which is always
    inside the gunicorn worker process.
    """
    global _initialized, _enabled, _mlflow_mod, _HAS_DISTRIBUTED_CTX

    if _initialized:
        return _enabled
    _initialized = True

    mlflow_cfg = getattr(store.config, "mlflow", None)
    if mlflow_cfg is None or not mlflow_cfg.enabled:
        return False
    if not mlflow_cfg.enable_tracing:
        return False

    try:
        import mlflow

        _mlflow_mod = mlflow
    except ImportError:
        _logger.debug("mlflow is not installed; tracing disabled")
        return False

    try:
        from mlflow.tracing import (  # noqa: F401
            set_tracing_context_from_http_request_headers,
        )

        _HAS_DISTRIBUTED_CTX = True
    except ImportError:
        _logger.debug(
            "mlflow.tracing.set_tracing_context_from_http_request_headers "
            "not available; parent-child linking disabled"
        )

    tracking_uri = mlflow_cfg.get_tracking_uri()
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)

    experiment_name = store.config.project
    try:
        mlflow.set_experiment(experiment_name)
    except Exception as exc:
        _logger.warning("Failed to set MLflow experiment %r: %s", experiment_name, exc)

    _enabled = True
    _logger.info(
        "Feast tracing initialized (mlflow, tracking_uri=%s, experiment=%s)",
        tracking_uri,
        experiment_name,
    )
    return True


@contextmanager
def traced_tool_span(
    store: "FeatureStore",
    name: str,
    attributes: Optional[Dict[str, str]] = None,
    request_headers: Optional[Dict[str, str]] = None,
) -> Iterator[Any]:
    """Context manager that creates an MLflow span for one API call.

    Lazily initializes tracing on first use.  If tracing is disabled
    or unavailable the body runs with ``span=None``.

    When *request_headers* contains a ``traceparent`` header, the span
    is created as a child of the caller's trace — enabling cross-process
    trace linking in the MLflow UI.
    """
    if not _lazy_init(store):
        yield None
        return

    has_traceparent = False
    try:
        has_traceparent = (
            _HAS_DISTRIBUTED_CTX
            and request_headers
            and "traceparent" in request_headers
        )
        if has_traceparent:
            from mlflow.tracing import (
                set_tracing_context_from_http_request_headers,
            )

            parent_ctx = set_tracing_context_from_http_request_headers(request_headers)
        else:
            parent_ctx = contextlib.nullcontext()
    except Exception:
        _logger.debug("Tracing setup failed", exc_info=True)
        yield None
        return

    with parent_ctx:
        with _mlflow_mod.start_span(name) as span:
            if attributes:
                for k, v in attributes.items():
                    span.set_attribute(k, v)
            yield span
        # Flush before parent_ctx exits — the parent context cleanup
        # removes the trace from MLflow's in-memory manager, so the
        # async exporter must send the span before that happens.
        if has_traceparent:
            try:
                _mlflow_mod.flush_trace_async_logging()
            except Exception:
                _logger.debug("flush_trace_async_logging failed", exc_info=True)
