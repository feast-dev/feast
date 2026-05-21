"""
Feast MCP server tracing via OpenTelemetry + MLflow.

Initializes an OTEL TracerProvider lazily (post-fork safe for gunicorn).
Two export modes, auto-detected:

  1. File-based (Milvus-lite safe):
     Spans export to local ./mlruns via MlflowSpanExporter.
     No HTTP calls, no background threads, no segfault risk.

  2. HTTP-based (production, non-embedded stores):
     Spans export to a remote MLflow tracking server.
     Enables cross-process trace stitching (Tier 3).

Auto-detection logic:
  - If online_store uses embedded milvus-lite (has ``path`` but no ``host``)
    AND tracking_uri is HTTP → force file-based mode with a warning.
  - Otherwise, use tracking_uri from config.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional

if TYPE_CHECKING:
    from feast.feature_store import FeatureStore

_logger = logging.getLogger(__name__)

_initialized = False
_enabled = False
_tracer: Any = None


def _is_embedded_store(store: "FeatureStore") -> bool:
    """Detect if the online store is an embedded C++ runtime (milvus-lite).

    Milvus-lite embeds a C++ runtime in-process. Combined with MLflow's HTTP
    export (background threads + network I/O) in a gunicorn-forked worker,
    this causes segfaults. We detect this case so we can disable HTTP export.
    """
    online_cfg = store.config.online_config
    if online_cfg is None:
        return False
    store_type = ""
    if isinstance(online_cfg, dict):
        store_type = online_cfg.get("type", "")
        has_path = bool(online_cfg.get("path"))
        has_host = bool(online_cfg.get("host"))
    else:
        store_type = getattr(online_cfg, "type", "") or ""
        has_path = bool(getattr(online_cfg, "path", None))
        has_host = bool(getattr(online_cfg, "host", None))

    if "milvus" not in store_type.lower():
        return False
    return has_path and not has_host


def _lazy_init(store: "FeatureStore") -> bool:
    """Initialize OTEL TracerProvider on first use (post-fork safe).

    Called from ``traced_tool_span`` the first time a traced endpoint is
    hit.  At this point the code runs inside the gunicorn worker, so
    importing heavy libraries and setting up providers is safe.
    """
    global _initialized, _enabled, _tracer

    if _initialized:
        return _enabled
    _initialized = True

    mlflow_cfg = store.config.mlflow
    if mlflow_cfg is None or not mlflow_cfg.enabled:
        return False
    if not mlflow_cfg.enable_tracing:
        return False

    try:
        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    except ImportError:
        _logger.debug("opentelemetry-sdk not installed; tracing disabled")
        return False

    tracking_uri = mlflow_cfg.get_tracking_uri()
    is_embedded = _is_embedded_store(store)
    is_http_uri = bool(tracking_uri and tracking_uri.startswith("http"))

    if is_embedded and is_http_uri:
        _logger.warning(
            "Embedded online store detected (milvus-lite). "
            "Forcing file-based tracing to avoid segfault. "
            "Use a remote Milvus cluster (online_store.host) for HTTP tracing."
        )
        tracking_uri = None

    try:
        import mlflow

        if tracking_uri:
            mlflow.set_tracking_uri(tracking_uri)
        mlflow.set_experiment(store.config.project)
    except Exception as e:
        _logger.warning("Failed to configure MLflow experiment: %s", e)

    try:
        from mlflow.tracing.export import MlflowV3SpanExporter as Exporter
    except ImportError:
        try:
            from mlflow.tracing.export import MlflowSpanExporter as Exporter
        except ImportError:
            _logger.debug("MLflow span exporter not available; tracing disabled")
            return False

    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(Exporter()))
    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer("feast.server")
    _enabled = True

    mode = "HTTP" if is_http_uri and not is_embedded else "file"
    _logger.info(
        "Feast tracing initialized (mode=%s, project=%s)", mode, store.config.project
    )
    return True


@contextmanager
def traced_tool_span(
    store: "FeatureStore",
    name: str,
    attributes: Optional[Dict[str, str]] = None,
) -> Iterator[Any]:
    """Context manager that creates a traced span for one tool call.

    Lazily initializes tracing on first use.  If tracing is disabled
    or unavailable the body runs with ``span=None``.

    The span is created within the current OTEL context, so if
    FastAPI OTEL instrumentation has set a parent span (from an
    incoming ``traceparent`` header), this span becomes a child of
    that parent — enabling cross-process trace linking.
    """
    if not _lazy_init(store):
        yield None
        return

    try:
        from opentelemetry import context, trace

        current_ctx = context.get_current()
        span = _tracer.start_span(name, context=current_ctx)
        if attributes:
            for k, v in attributes.items():
                span.set_attribute(k, v)
        token = context.attach(trace.set_span_in_context(span))
        try:
            yield span
        finally:
            span.end()
            context.detach(token)
    except Exception as exc:
        _logger.debug("Traced tool span failed: %s", exc)
        yield None
