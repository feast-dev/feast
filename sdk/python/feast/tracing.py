"""
MLflow tracing for Feast feature server.

Each MCP tool call produces one trace with a single span containing
full Feast metadata (project, features, entity counts, etc.).

Tracing is initialized lazily on first span creation (post-fork in
gunicorn workers) to avoid conflicts with forked processes.  Uses local
file-based tracing (``./mlruns``) which is safe with embedded stores
like Milvus-lite.  View traces with ``mlflow ui`` from the server dir.
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


def _lazy_init(store: "FeatureStore") -> bool:
    """Initialize tracing on first use (post-fork safe).

    Called from ``traced_tool_span`` the first time a traced endpoint
    is hit.  At this point the code is running inside the gunicorn
    worker, so importing MLflow and starting spans is safe.
    """
    global _initialized, _enabled

    if _initialized:
        return _enabled
    _initialized = True

    mlflow_cfg = getattr(store.config, "mlflow", None)
    if mlflow_cfg is None or not mlflow_cfg.enabled:
        return False
    if not getattr(mlflow_cfg, "enable_tracing", True):
        return False

    try:
        import mlflow

        if hasattr(mlflow, "start_span"):
            _enabled = True
            _logger.info("Feast tracing initialized (MLflow native, post-fork)")
            return True
    except ImportError:
        pass

    return False


@contextmanager
def traced_tool_span(
    store: "FeatureStore",
    name: str,
    attributes: Optional[Dict[str, str]] = None,
) -> Iterator[Any]:
    """Context manager that creates a traced span for one tool call.

    Lazily initializes tracing on first use.  If tracing is disabled
    or unavailable the body runs with ``span=None``.
    """
    if not _lazy_init(store):
        yield None
        return

    try:
        import mlflow

        with mlflow.start_span(name=name) as span:
            if attributes:
                span.set_attributes(attributes)
            yield span
    except Exception as exc:
        _logger.debug("Traced tool span failed: %s", exc)
        yield None
