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
import os
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional

if TYPE_CHECKING:
    from feast.feature_store import FeatureStore

_logger = logging.getLogger(__name__)

_initialized = False
_enabled = False
_mlflow_mod: Any = None

_HAS_DISTRIBUTED_CTX = False

_FEAST_VERSION: Optional[str] = None


def _lazy_init(store: "FeatureStore") -> bool:
    """Initialize MLflow tracing on first use (post-fork safe).

    Only sets ``tracking_uri`` and ``experiment`` — no sockets, no
    threads, no gRPC channels.  ``mlflow.start_span`` lazily creates
    its internal OTEL machinery on the first span, which is always
    inside the gunicorn worker process.
    """
    global _initialized, _enabled, _mlflow_mod, _HAS_DISTRIBUTED_CTX, _FEAST_VERSION

    if _initialized:
        return _enabled
    _initialized = True

    mlflow_cfg = getattr(store.config, "mlflow", None)
    if mlflow_cfg is None or not mlflow_cfg.enabled:
        return False
    if not mlflow_cfg.enable_distributed_tracing:
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

    sampling = getattr(mlflow_cfg, "trace_sampling_ratio", None)
    if isinstance(sampling, (int, float)) and sampling < 1.0:
        os.environ.setdefault("MLFLOW_TRACE_SAMPLING_RATIO", str(sampling))

    try:
        import feast

        _FEAST_VERSION = feast.__version__
    except Exception:
        pass

    # Install span processors from config (LLM context tagging + optional PII
    # redaction).  Safe no-op when mlflow.tracing.configure is unavailable.
    try:
        from feast.tracing_hooks import install_feast_span_processor

        install_feast_span_processor(
            redact_pii=bool(getattr(mlflow_cfg, "redact_entity_pii", False))
        )
    except Exception:
        _logger.debug("Failed to install Feast span processor(s)", exc_info=True)

    _enabled = True
    _logger.info(
        "Feast tracing initialized (mlflow, tracking_uri=%s, experiment=%s, "
        "redact_entity_pii=%s)",
        tracking_uri,
        experiment_name,
        bool(getattr(mlflow_cfg, "redact_entity_pii", False)),
    )
    return True


@contextmanager
def traced_tool_span(
    store: "FeatureStore",
    name: str,
    attributes: Optional[Dict[str, str]] = None,
    request_headers: Optional[Dict[str, str]] = None,
    span_type: str = "TOOL",
    inputs: Optional[Dict[str, Any]] = None,
    session_id: Optional[str] = None,
    user: Optional[str] = None,
) -> Iterator[Any]:
    """Context manager that creates an MLflow span for one API call.

    Lazily initializes tracing on first use.  If tracing is disabled
    or unavailable the body runs with ``span=None``.

    When *request_headers* contains a ``traceparent`` header, the span
    is created as a child of the caller's trace — enabling cross-process
    trace linking in the MLflow UI.

    Args:
        store: The FeatureStore instance (used for lazy init config).
        name: Span name (e.g. ``"feast.get_online_features"``).
        attributes: Key-value pairs set as span attributes.
        request_headers: HTTP headers from the incoming request.
        span_type: MLflow SpanType string (``"TOOL"``, ``"RETRIEVER"``, etc.).
        inputs: Dict recorded as span inputs (visible in MLflow UI).
        session_id: MCP session ID — set on the trace for session grouping.
        user: Username — set on the trace for user-level filtering.
    """
    if not _lazy_init(store):
        yield None
        return

    has_traceparent = bool(
        _HAS_DISTRIBUTED_CTX and request_headers and "traceparent" in request_headers
    )
    try:
        parent_ctx: Any = contextlib.nullcontext()
        if has_traceparent:
            from mlflow.tracing import (
                set_tracing_context_from_http_request_headers,
            )

            parent_ctx = set_tracing_context_from_http_request_headers(
                request_headers  # type: ignore[arg-type]
            )
    except Exception:
        _logger.debug("Tracing setup failed", exc_info=True)
        yield None
        return

    with parent_ctx:
        with _mlflow_mod.start_span(name, span_type=span_type) as span:
            if inputs:
                try:
                    span.set_inputs(inputs)
                except Exception:
                    _logger.debug("Failed to set span inputs", exc_info=True)
            if attributes:
                for k, v in attributes.items():
                    span.set_attribute(k, v)
            if _FEAST_VERSION:
                span.set_attribute("feast.sdk_version", _FEAST_VERSION)

            _update_trace_metadata(session_id, user, attributes)

            try:
                yield span
            except Exception as exc:
                try:
                    span.record_exception(exc)
                except Exception:
                    _logger.debug("Failed to record exception on span", exc_info=True)
                raise

        # Flush before parent_ctx exits — the parent context cleanup
        # removes the trace from MLflow's in-memory manager, so the
        # async exporter must send the span before that happens.
        if has_traceparent:
            try:
                _mlflow_mod.flush_trace_async_logging()
            except Exception:
                _logger.debug("flush_trace_async_logging failed", exc_info=True)


def _update_trace_metadata(
    session_id: Optional[str],
    user: Optional[str],
    attributes: Optional[Dict[str, str]],
) -> None:
    """Set trace-level session, user, and tags via ``update_current_trace``."""
    if _mlflow_mod is None:
        return

    update_kwargs: Dict[str, Any] = {}
    if session_id:
        update_kwargs["session_id"] = session_id
    if user:
        update_kwargs["user"] = user

    tags: Dict[str, str] = {}
    if attributes:
        project = attributes.get("feast.project")
        if project:
            tags["feast.project"] = project
        rtype = attributes.get("feast.retrieval_type")
        if rtype:
            tags["feast.retrieval_type"] = rtype
    if _FEAST_VERSION:
        tags["feast.sdk_version"] = _FEAST_VERSION
    env = os.environ.get("FEAST_ENVIRONMENT")
    if env:
        tags["feast.environment"] = env

    if tags:
        update_kwargs["tags"] = tags

    if not update_kwargs:
        return

    try:
        _mlflow_mod.update_current_trace(**update_kwargs)
    except Exception:
        _logger.debug("Failed to update trace metadata", exc_info=True)
