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

    For advanced use cases, the module also provides:
    - resolve_feature_service_from_model_uri: Map an MLflow model to its Feast
      feature service.
    - get_entity_df_from_mlflow_run: Reproduce training by pulling entity data
      from a previous MLflow run's artifacts.
"""

from feast.mlflow_integration.client import FeastMlflowClient
from feast.mlflow_integration.config import MlflowConfig
from feast.mlflow_integration.entity_df_builder import (
    FeastMlflowEntityDfError,
    get_entity_df_from_mlflow_run,
)
from feast.mlflow_integration.logger import (
    log_apply_to_mlflow,
    log_feature_retrieval_to_mlflow,
    log_materialize_to_mlflow,
    log_training_dataset_to_mlflow,
)
from feast.mlflow_integration.model_resolver import (
    FeastMlflowModelResolutionError,
    resolve_feature_service_from_model_uri,
)

__all__ = [
    "FeastMlflowClient",
    "MlflowConfig",
    "log_feature_retrieval_to_mlflow",
    "log_training_dataset_to_mlflow",
    "log_apply_to_mlflow",
    "log_materialize_to_mlflow",
    "resolve_feature_service_from_model_uri",
    "FeastMlflowModelResolutionError",
    "get_entity_df_from_mlflow_run",
    "FeastMlflowEntityDfError",
]
