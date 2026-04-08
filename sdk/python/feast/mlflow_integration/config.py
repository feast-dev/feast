from typing import Optional

from pydantic import StrictBool, StrictStr

from feast.repo_config import FeastBaseModel


class MlflowConfig(FeastBaseModel):
    """Configuration for MLflow integration.

    This enables automatic logging of feature retrieval metadata to MLflow
    during get_historical_features and get_online_features calls.

    Example configuration in feature_store.yaml:
        mlflow:
            enabled: true
            tracking_uri: http://localhost:5000
            auto_log: true
    """

    enabled: StrictBool = False
    """ bool: Whether MLflow integration is enabled. Defaults to False. """

    tracking_uri: Optional[StrictStr] = None
    """ str: MLflow tracking URI. If not set, uses MLflow's default
        (MLFLOW_TRACKING_URI env var or local ./mlruns). """

    auto_log: StrictBool = True
    """ bool: Automatically log feature retrieval metadata to the active
        MLflow run when get_historical_features or get_online_features is
        called. Defaults to True. """

    auto_log_dataset: StrictBool = False
    """ bool: When True, the training DataFrame produced by
        get_historical_features().to_df() is logged as an MLflow dataset
        input on the active run. Defaults to False because the DataFrame
        can be large. """
