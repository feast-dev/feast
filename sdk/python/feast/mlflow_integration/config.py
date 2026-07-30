import os
from typing import Dict, Optional

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

    enable_distributed_tracing: StrictBool = True
    """ bool: When True and mlflow.enabled=True, server-side API calls
        create MLflow trace spans via mlflow.start_span().  Spans appear
        in the MLflow UI Traces tab and support parent-child linking via
        traceparent headers.  Defaults to True. """

    trace_sampling_ratio: float = 1.0
    """ float: Fraction of requests to trace (0.0–1.0).  Set below 1.0
        for high-volume production to reduce overhead.  Defaults to 1.0
        (trace every request). Maps to MLFLOW_TRACE_SAMPLING_RATIO. """

    redact_entity_pii: StrictBool = False
    """ bool: When True, entity values in span inputs are replaced with
        ``[REDACTED]`` before the span is sent to MLflow.  Useful when
        entity keys contain PII.  Defaults to False. """

    dataset_sync: DatasetSyncConfig = DatasetSyncConfig()
    """ DatasetSyncConfig: Configuration for the ``feast mlflow sync-dataset``
        command (field mapping, watermark key, batch size). """

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
