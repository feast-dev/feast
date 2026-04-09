from typing import Optional

from pydantic import StrictBool, StrictStr

from feast.repo_config import FeastBaseModel


class MlflowConfig(FeastBaseModel):
    enabled: StrictBool = False
    """ bool: Whether MLflow integration is enabled. Defaults to False. """

    tracking_uri: Optional[StrictStr] = None
    """ str: MLflow tracking URI. If not set, defaults to
        http://127.0.0.1:5000 (local MLflow tracking server).
        Set explicitly for remote/shared MLflow deployments. """

    auto_log: StrictBool = True
    """ bool: Automatically log feature retrieval metadata to the active
        MLflow run when get_historical_features or get_online_features is
        called. Defaults to True. """

    auto_log_entity_df: StrictBool = False
    """ bool: When True, the input entity_df (or SQL query) is recorded in
        the MLflow run. Defaults to False. """
