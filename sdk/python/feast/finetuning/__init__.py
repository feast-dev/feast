"""Fine-tuning dataset utilities for Feast.

Extract prompt/completion pairs from MLflow traces, join with human
feedback labels from a Feast :class:`~feast.labeling.label_view.LabelView`,
and export fine-tuning JSONL for LLM training.
"""

from feast.finetuning.trace_extractor import FinetuningExample, extract_from_traces

__all__ = [
    "FinetuningExample",
    "extract_from_traces",
    "resolve_labels_from_feast",
    "resolve_labels_from_mlflow",
    "filter_labeled_only",
    "get_exporter",
]


def __getattr__(name: str):
    """Lazy imports for modules that depend on optional packages."""
    if name == "resolve_labels_from_feast":
        from feast.finetuning.label_resolver import resolve_labels_from_feast

        return resolve_labels_from_feast
    if name == "resolve_labels_from_mlflow":
        from feast.finetuning.label_resolver import resolve_labels_from_mlflow

        return resolve_labels_from_mlflow
    if name == "filter_labeled_only":
        from feast.finetuning.label_resolver import filter_labeled_only

        return filter_labeled_only
    if name == "get_exporter":
        from feast.finetuning.exporters import get_exporter

        return get_exporter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
