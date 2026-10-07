"""
MLflow integration for Feast Feature Store.

This module provides seamless integration between Feast and MLflow. When enabled
in feature_store.yaml, feature metadata is logged to MLflow
during get_historical_features and get_online_features calls.

Usage:
    Configure MLflow in your feature_store.yaml:

        project: my_project
        # ... other config ...

        mlflow:
            enabled: true
            tracking_uri: https://mlflow.example.com  # or set MLFLOW_TRACKING_URI
            auto_log: true

    When ``tracking_uri`` is omitted, the ``MLFLOW_TRACKING_URI`` environment
    variable is used. If neither is set, MLflow falls back to its own default.

    All functionality is accessed through ``store.mlflow``:

    - ``store.mlflow.start_run()`` — start an MLflow run pre-tagged with Feast metadata
    - ``store.mlflow.log_model()`` — log a model with ``feast_features.json``
    - ``store.mlflow.resolve_features()`` — map an MLflow model to its feature service
    - ``store.mlflow.get_training_entity_df()`` — reproduce training by pulling entity
      data from a previous MLflow run's artifacts
"""

from feast.mlflow_integration.config import MlflowConfig
from feast.mlflow_integration.entity_df_builder import FeastMlflowEntityDfError
from feast.mlflow_integration.model_resolver import FeastMlflowModelResolutionError
from feast.trace_export.trace_extractor import (
    TraceExportExample,
    extract_from_traces,
)

__all__ = [
    "MlflowConfig",
    "FeastMlflowModelResolutionError",
    "FeastMlflowEntityDfError",
    "TraceExportExample",
    "extract_from_traces",
    "resolve_labels_from_feast",
    "resolve_labels_from_mlflow",
    "filter_labeled_only",
    "get_exporter",
]


def __getattr__(name: str):
    """Lazy imports for trace export modules that depend on optional packages."""
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
