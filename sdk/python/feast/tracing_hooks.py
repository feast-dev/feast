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


def feast_pii_redactor(span: Any) -> None:
    """Replace entity values with ``[REDACTED]`` in span inputs.

    Registered alongside :func:`feast_span_processor` when the
    ``redact_entity_pii`` config option is enabled.
    """
    try:
        inputs = getattr(span, "inputs", None)
        if not isinstance(inputs, dict):
            return
        entities = inputs.get("entities")
        if entities and isinstance(entities, dict):
            redacted = {k: "[REDACTED]" for k in entities}
            span.set_inputs({**inputs, "entities": redacted})
    except Exception:
        _logger.debug("PII redaction failed on span", exc_info=True)


def install_feast_span_processor(redact_pii: bool = False) -> None:
    """Register Feast span processors with the MLflow tracing system.

    Args:
        redact_pii: When ``True``, also install :func:`feast_pii_redactor`
            to scrub entity values from span inputs.
    """
    try:
        import mlflow.tracing

        processors = [feast_span_processor]
        if redact_pii:
            processors.append(feast_pii_redactor)
        mlflow.tracing.configure(span_processors=processors)
        _logger.debug(
            "Feast span processor(s) installed (pii_redaction=%s)", redact_pii
        )
    except (ImportError, AttributeError):
        _logger.debug(
            "mlflow.tracing.configure not available; span processor not installed"
        )
