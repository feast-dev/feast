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

__all__ = [
    "MlflowConfig",
    "FeastMlflowModelResolutionError",
    "FeastMlflowEntityDfError",
]
