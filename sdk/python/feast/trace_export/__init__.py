"""Trace export pipeline: extract, label, and export fine-tuning data."""

from feast.trace_export.trace_extractor import (
    TraceExportExample,
    extract_from_traces,
)

__all__ = [
    "TraceExportExample",
    "extract_from_traces",
    "resolve_labels_from_feast",
    "resolve_labels_from_mlflow",
    "filter_labeled_only",
    "get_exporter",
]


def __getattr__(name: str):
    """Lazy imports for modules that depend on optional packages."""
    if name == "resolve_labels_from_feast":
        from feast.trace_export.label_resolver import resolve_labels_from_feast

        return resolve_labels_from_feast
    if name == "resolve_labels_from_mlflow":
        from feast.trace_export.label_resolver import resolve_labels_from_mlflow

        return resolve_labels_from_mlflow
    if name == "filter_labeled_only":
        from feast.trace_export.label_resolver import filter_labeled_only

        return filter_labeled_only
    if name == "get_exporter":
        from feast.trace_export.exporters import get_exporter

        return get_exporter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
