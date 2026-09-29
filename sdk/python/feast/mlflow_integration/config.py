import os
from typing import Dict, List, Optional

from pydantic import StrictBool, StrictInt, StrictStr

from feast.repo_config import FeastBaseModel

MLFLOW_TAG_MAX_LENGTH = 5000
MLFLOW_TAG_TRUNCATION_LIMIT = MLFLOW_TAG_MAX_LENGTH - 10
MLFLOW_TAG_TRUNCATION_SLICE = MLFLOW_TAG_MAX_LENGTH - 13

MLFLOW_PARAM_MAX_LENGTH = 500
MLFLOW_PARAM_TRUNCATION_LIMIT = MLFLOW_PARAM_MAX_LENGTH - 10
MLFLOW_PARAM_TRUNCATION_SLICE = MLFLOW_PARAM_MAX_LENGTH - 13

DEFAULT_ENTITY_DF_MAX_ROWS = 100_000
WATERMARK_TAG_KEY = "feast_last_sync_time"
DEFAULT_BATCH_SIZE = 10_000


def resolve_tracking_uri(configured_uri: Optional[str] = None) -> Optional[str]:
    """Return the effective MLflow tracking URI.

    Priority:
      1. Explicitly configured URI from feature_store.yaml
      2. MLFLOW_TRACKING_URI environment variable (MLflow's native convention)
      3. None — let MLflow fall back to its own defaults (local ./mlruns)
    """
    if configured_uri:
        return configured_uri
    return os.environ.get("MLFLOW_TRACKING_URI")


class DatasetSyncConfig(FeastBaseModel):
    """Configuration for the ``feast mlflow sync-dataset`` command."""

    default_field_mapping: Dict[str, str] = {}
    """ dict: Default field mapping overrides applied during dataset sync.
        Keys are dot-delimited MLflow paths (e.g. 'expectations.expected_response'),
        values are target Feast column names. """

    watermark_key: StrictStr = WATERMARK_TAG_KEY
    """ str: MLflow dataset tag key used to track the last sync timestamp
        for incremental syncing. Defaults to 'feast_last_sync_time'. """

    default_batch_size: StrictInt = DEFAULT_BATCH_SIZE
    """ int: Default batch size for write_to_online_store during sync.
        Defaults to 10000. """


DEFAULT_REQUEST_TIMEOUT = 30
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_BACKOFF_FACTOR = 1.0


def resolve_ui_url(configured_url: Optional[str] = None) -> Optional[str]:
    """Return the browser-reachable MLflow UI URL for hyperlinks.

    Priority:
      1. Explicitly configured ui_url from feature_store.yaml
      2. MLFLOW_UI_URL environment variable
      3. None — callers fall back to tracking_uri (works for local dev)
    """
    if configured_url:
        return configured_url
    return os.environ.get("MLFLOW_UI_URL")


class MlflowConfig(FeastBaseModel):
    enabled: StrictBool = False
    """ bool: Whether MLflow integration is enabled. Defaults to False. """

    tracking_uri: Optional[StrictStr] = None
    """ str: MLflow tracking URI. When not set, the MLFLOW_TRACKING_URI
        environment variable is used. If neither is set, MLflow falls back
        to its own default (local ./mlruns directory). """

    ui_url: Optional[StrictStr] = None
    """ str: Browser-reachable MLflow UI URL used for hyperlinks in Feast UI
        lineage pages. When not set, the MLFLOW_UI_URL environment variable
        is used. If neither is set, falls back to tracking_uri. On
        operator-managed deployments, this is auto-discovered from the MLflow
        CR status.url (the external gateway route). """

    auto_log: StrictBool = True
    """ bool: Automatically log feature retrieval metadata to the active
        MLflow run when get_historical_features or get_online_features is
        called. Defaults to True. """

    auto_log_entity_df: StrictBool = False
    """ bool: When True, the input entity_df (or SQL query) is recorded in
        the MLflow run. Defaults to False. """

    entity_df_max_rows: StrictInt = DEFAULT_ENTITY_DF_MAX_ROWS
    """ int: Maximum number of entity DataFrame rows to save as an MLflow
        artifact. DataFrames exceeding this limit are skipped to avoid
        OOM and slow uploads. Defaults to 100000. """

    log_operations: StrictBool = False
    """ bool: Log feast apply and materialize operations to a separate
        MLflow experiment. Opt-in to avoid noise. Defaults to False. """

    ops_experiment_suffix: StrictStr = "-feast-ops"
    """ str: Suffix appended to the project name to form the MLflow
        experiment name for operation logs. Defaults to '-feast-ops'. """

    dataset_sync: DatasetSyncConfig = DatasetSyncConfig()
    """ DatasetSyncConfig: Configuration for the ``feast mlflow sync-dataset``
        command (field mapping, watermark key, batch size). """

    ca_bundle: Optional[StrictStr] = None
    """ str: Path to a CA bundle for TLS verification when connecting to
        the MLflow tracking server (e.g. /etc/pki/tls/odh-trusted-ca-bundle.crt).
        When not set, falls back to the REQUESTS_CA_BUNDLE env var or system
        defaults. """

    supported_artifact_formats: List[StrictStr] = ["parquet", "csv"]
    """ list[str]: Artifact formats the MlflowDatasetSource adapter will
        accept.  Unsupported formats raise a clear error at validation time. """

    request_timeout: StrictInt = DEFAULT_REQUEST_TIMEOUT
    """ int: Timeout in seconds for individual HTTP requests to the MLflow
        tracking server. Applies to artifact downloads and GenAI dataset
        fetches. Defaults to 30. """

    max_retries: StrictInt = DEFAULT_MAX_RETRIES
    """ int: Maximum number of retry attempts for transient MLflow API
        failures (timeouts, 5xx, connection errors). Uses exponential
        backoff. Defaults to 3. """

    retry_backoff_factor: float = DEFAULT_RETRY_BACKOFF_FACTOR
    """ float: Base factor for exponential backoff between retries.
        Wait time = backoff_factor * (2 ** attempt). Defaults to 1.0,
        giving waits of 1s, 2s, 4s. """

    def get_tracking_uri(self) -> Optional[str]:
        """Resolve the effective tracking URI for this config instance."""
        return resolve_tracking_uri(self.tracking_uri)

    def get_ui_url(self) -> Optional[str]:
        """Resolve the browser-reachable UI URL for lineage hyperlinks.

        Falls back to tracking_uri when no explicit ui_url is configured,
        which is correct for local/non-operator deployments where the
        tracking URI is already browser-reachable.
        """
        return resolve_ui_url(self.ui_url) or self.get_tracking_uri()
