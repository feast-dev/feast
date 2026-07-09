"""Extract fine-tuning examples from MLflow traces with Feast context.

Queries an MLflow experiment for traces containing CHAT_MODEL spans,
extracts prompt/completion pairs and ``feast.*`` span attributes, and
returns a list of :class:`FinetuningExample` objects ready for label
resolution and export.
"""

import json
import logging
import warnings
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class FinetuningExample:
    """A single fine-tuning training example extracted from an MLflow trace.

    Flows through the entire pipeline: extractor → label_resolver → exporter.
    Label fields (``label``, ``corrected_response``, ``labeler``) are populated
    by :mod:`feast.finetuning.label_resolver` after extraction.
    """

    trace_id: str
    messages: List[Dict[str, str]]
    original_completion: Optional[str] = None
    label: Optional[str] = None
    corrected_response: Optional[str] = None
    labeler: Optional[str] = None
    feature_refs: List[str] = field(default_factory=list)
    feature_views: List[str] = field(default_factory=list)
    entity_values: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


def _require_mlflow():
    """Import and return the ``mlflow`` module, raising a clear error if absent."""
    try:
        import mlflow

        return mlflow
    except ImportError:
        raise ImportError(
            "The 'mlflow' package is required for fine-tuning export. "
            "Install it with: pip install mlflow"
        )


def extract_from_traces(
    tracking_uri: str,
    experiment_name: str,
    filter_string: Optional[str] = None,
    max_results: int = 1000,
) -> List[FinetuningExample]:
    """Query MLflow traces and extract fine-tuning examples.

    Args:
        tracking_uri: MLflow tracking server URI.
        experiment_name: Name of the MLflow experiment to query.
        filter_string: Optional MLflow ``search_traces`` filter expression.
        max_results: Maximum number of traces to retrieve.

    Returns:
        A list of :class:`FinetuningExample` with prompt/completion pairs
        extracted. Label fields are ``None`` — populate them via
        :func:`~feast.finetuning.label_resolver.resolve_labels_from_feast`
        or :func:`~feast.finetuning.label_resolver.resolve_labels_from_mlflow`.
    """
    mlflow = _require_mlflow()
    mlflow.set_tracking_uri(tracking_uri)

    experiment = mlflow.get_experiment_by_name(experiment_name)
    if experiment is None:
        raise ValueError(
            f"MLflow experiment '{experiment_name}' not found at {tracking_uri}"
        )

    traces_df = mlflow.search_traces(
        experiment_ids=[experiment.experiment_id],
        max_results=max_results,
        **({"filter_string": filter_string} if filter_string else {}),
    )

    examples: List[FinetuningExample] = []
    skipped = 0

    for _, row in traces_df.iterrows():
        trace_id = row.get("trace_id") or row.get("request_id") or "unknown"
        try:
            trace = mlflow.get_trace(str(trace_id))
            example = _process_trace(trace)
            if example is not None:
                examples.append(example)
            else:
                skipped += 1
        except Exception:
            logger.warning("Failed to process trace %s", trace_id, exc_info=True)
            skipped += 1

    if skipped:
        warnings.warn(
            f"Skipped {skipped} trace(s) that lacked CHAT_MODEL spans or "
            f"feast.* attributes.",
            stacklevel=2,
        )

    examples.sort(key=lambda ex: ex.trace_id)
    return examples


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _get_trace_id(trace: Any) -> str:
    """Extract trace_id from an MLflow Trace object (DataFrame row or object)."""
    if hasattr(trace, "info"):
        return trace.info.request_id
    if hasattr(trace, "request_id"):
        return trace.request_id
    return str(trace.get("request_id", "unknown"))


def _get_spans(trace: Any) -> List[Any]:
    """Return the list of spans from an MLflow trace."""
    if hasattr(trace, "data") and hasattr(trace.data, "spans"):
        return list(trace.data.spans)
    if hasattr(trace, "search_traces_data"):
        return list(trace.search_traces_data.spans)
    return []


def _process_trace(trace: Any) -> Optional[FinetuningExample]:
    """Convert a single MLflow trace into a FinetuningExample, or None."""
    trace_id = _get_trace_id(trace)
    spans = _get_spans(trace)
    if not spans:
        return None

    chat_spans = _find_spans_by_type(spans, "CHAT_MODEL")
    if not chat_spans:
        return None

    chat_span = chat_spans[0]

    messages = _extract_messages(chat_span)
    if not messages:
        return None

    completion = _extract_completion_text(chat_span)

    feast_attrs = _collect_feast_attrs(spans, chat_span)
    feature_refs = _parse_csv_attr(feast_attrs.get("feast.context_features", ""))
    feature_views = _parse_csv_attr(feast_attrs.get("feast.context_feature_views", ""))

    root_span = _find_root_span(spans)
    entity_values = _extract_entity_values(root_span)

    metadata = _build_metadata(chat_span, trace)

    return FinetuningExample(
        trace_id=trace_id,
        messages=messages,
        original_completion=completion,
        feature_refs=feature_refs,
        feature_views=feature_views,
        entity_values=entity_values,
        metadata=metadata,
    )


def _find_spans_by_type(spans: List[Any], span_type: str) -> List[Any]:
    """Filter spans whose ``mlflow.spanType`` attribute matches *span_type*."""
    result = []
    for span in spans:
        attrs = _get_span_attributes(span)
        if attrs.get("mlflow.spanType") == span_type:
            result.append(span)
    return result


def _find_parent_span(spans: List[Any], child: Any) -> Optional[Any]:
    """Walk up the span tree to find *child*'s parent."""
    parent_id = _get_parent_id(child)
    if parent_id is None:
        return None
    for span in spans:
        if _get_span_id(span) == parent_id:
            return span
    return None


def _find_root_span(spans: List[Any]) -> Optional[Any]:
    """Return the span with no parent (the root)."""
    for span in spans:
        if _get_parent_id(span) is None:
            return span
    return spans[0] if spans else None


def _get_span_attributes(span: Any) -> Dict[str, Any]:
    if hasattr(span, "attributes"):
        attrs = span.attributes
        return dict(attrs) if attrs else {}
    return {}


def _get_span_id(span: Any) -> Optional[str]:
    if hasattr(span, "span_id"):
        return span.span_id
    if hasattr(span, "context") and hasattr(span.context, "span_id"):
        return span.context.span_id
    return None


def _get_parent_id(span: Any) -> Optional[str]:
    if hasattr(span, "parent_id"):
        return span.parent_id
    return None


def _extract_messages(chat_span: Any) -> List[Dict[str, str]]:
    """Extract the messages list from a CHAT_MODEL span's inputs."""
    attrs = _get_span_attributes(chat_span)
    inputs_raw = attrs.get("mlflow.spanInputs", "{}")
    inputs = _parse_json(inputs_raw)

    if isinstance(inputs, dict):
        msgs = inputs.get("messages", [])
        if isinstance(msgs, list):
            return [
                {"role": m.get("role", "user"), "content": m.get("content", "")}
                for m in msgs
                if isinstance(m, dict)
            ]
    return []


def _extract_completion_text(chat_span: Any) -> Optional[str]:
    """Extract the assistant's completion text from a CHAT_MODEL span's outputs."""
    attrs = _get_span_attributes(chat_span)
    outputs_raw = attrs.get("mlflow.spanOutputs", "{}")
    outputs = _parse_json(outputs_raw)

    if isinstance(outputs, dict):
        choices = outputs.get("choices", [])
        if choices and isinstance(choices, list):
            first = choices[0]
            if isinstance(first, dict):
                msg = first.get("message", {})
                if isinstance(msg, dict):
                    return msg.get("content")
    return None


def _collect_feast_attrs(spans: List[Any], chat_span: Any) -> Dict[str, str]:
    """Collect ``feast.*`` attributes from the chat span and its ancestors."""
    feast_attrs: Dict[str, str] = {}
    current: Optional[Any] = chat_span
    while current is not None:
        attrs = _get_span_attributes(current)
        for key, val in attrs.items():
            if key.startswith("feast.") and key not in feast_attrs:
                feast_attrs[key] = str(val)
        current = _find_parent_span(spans, current)
    return feast_attrs


def _extract_entity_values(span: Optional[Any]) -> Dict[str, Any]:
    """Best-effort extraction of entity key/values from the root span inputs."""
    if span is None:
        return {}
    attrs = _get_span_attributes(span)
    inputs_raw = attrs.get("mlflow.spanInputs", "{}")
    inputs = _parse_json(inputs_raw)
    if isinstance(inputs, dict):
        return {k: v for k, v in inputs.items() if not k.startswith("mlflow.")}
    return {}


def _build_metadata(chat_span: Any, trace: Any) -> Dict[str, Any]:
    """Build a metadata dict with model info, token counts, timestamps, and assessments."""
    attrs = _get_span_attributes(chat_span)
    meta: Dict[str, Any] = {}

    for key in ("model", "temperature", "input_tokens", "output_tokens"):
        qualified = f"mlflow.chat.{key}" if f"mlflow.chat.{key}" in attrs else key
        if qualified in attrs:
            meta[key] = attrs[qualified]
        elif key in attrs:
            meta[key] = attrs[key]

    if hasattr(trace, "info"):
        info = trace.info
        if hasattr(info, "timestamp_ms"):
            meta["trace_timestamp_ms"] = info.timestamp_ms
        if hasattr(info, "request_time"):
            meta["trace_timestamp_ms"] = info.request_time

    expectations = _extract_assessments(trace, kind="expectation")
    if expectations:
        meta["expectations"] = expectations

    feedback = _extract_assessments(trace, kind="feedback")
    if feedback:
        meta["feedback"] = feedback

    return meta


def _extract_assessments(trace: Any, kind: str = "expectation") -> Dict[str, Any]:
    """Extract expectation or feedback assessments from a trace.

    Args:
        trace: An MLflow Trace object.
        kind: Either ``"expectation"`` or ``"feedback"``.

    Returns:
        A dict mapping assessment names to their values.
        For expectations: ``{"expected_response": "The correct answer..."}``
        For feedback: ``{"response_quality": "poor"}``
    """
    result: Dict[str, Any] = {}

    assessments = None
    if hasattr(trace, "info") and hasattr(trace.info, "assessments"):
        assessments = trace.info.assessments

    if not assessments:
        return result

    for assessment in assessments:
        if kind == "expectation":
            expectation_val = getattr(assessment, "expectation", None)
            if expectation_val is not None:
                value = getattr(expectation_val, "value", None)
                if value is not None:
                    name = getattr(assessment, "name", "expected_response")
                    result[name] = value
        elif kind == "feedback":
            feedback_val = getattr(assessment, "feedback", None)
            if feedback_val is not None:
                value = getattr(feedback_val, "value", None)
                if value is not None:
                    name = getattr(assessment, "name", "feedback")
                    entry: Dict[str, Any] = {"value": value}
                    source = getattr(assessment, "source", None)
                    if source is not None:
                        source_id = getattr(source, "source_id", None)
                        if source_id:
                            entry["source_id"] = source_id
                    rationale = getattr(assessment, "rationale", None)
                    if rationale:
                        entry["rationale"] = rationale
                    result[name] = entry

    return result


def _parse_csv_attr(value: str) -> List[str]:
    """Split a comma-separated attribute value into a list of strings."""
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _parse_json(value: Any) -> Any:
    """Parse a JSON string, returning the original value on failure."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            return value
    return value
