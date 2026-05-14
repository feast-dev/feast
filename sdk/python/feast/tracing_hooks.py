"""
MLflow SpanProcessor that tags LLM spans with Feast feature context.

Install via ``install_feast_span_processor()`` which registers a callable
with ``mlflow.tracing.configure(span_processors=[...])``.  The processor
only fires on ``LLM`` / ``CHAT_MODEL`` span types and reads the thread-local
``FeastTraceContext`` populated during feature retrieval.
"""

from __future__ import annotations

import logging
from typing import Any

_logger = logging.getLogger(__name__)

_LLM_SPAN_TYPES = {"LLM", "CHAT_MODEL"}


def feast_span_processor(span: Any) -> None:
    """Callable for ``mlflow.tracing.configure(span_processors=[...])``."""
    span_type = getattr(span, "span_type", None)
    if span_type not in _LLM_SPAN_TYPES:
        return

    from feast.tracing_context import get_current_context

    ctx = get_current_context()
    if ctx is None or not ctx.feature_refs:
        return

    attrs = ctx.get_context_attributes()
    for key, value in attrs.items():
        try:
            span.set_attribute(key, value)
        except Exception:
            _logger.debug("Failed to set attribute %s on LLM span", key)


def install_feast_span_processor() -> None:
    """Register ``feast_span_processor`` with the MLflow tracing system."""
    try:
        import mlflow.tracing

        mlflow.tracing.configure(span_processors=[feast_span_processor])
        _logger.debug("Feast span processor installed")
    except (ImportError, AttributeError):
        _logger.debug(
            "mlflow.tracing.configure not available; span processor not installed"
        )
