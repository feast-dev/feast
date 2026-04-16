from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, List, Optional

import pandas as pd

from feast.mlflow_integration.config import (
    MLFLOW_TAG_TRUNCATION_LIMIT,
    MLFLOW_TAG_TRUNCATION_SLICE,
)

if TYPE_CHECKING:
    from feast.feature_service import FeatureService

_logger = logging.getLogger(__name__)

_mlflow = None
_mlflow_checked = False

_consecutive_failures = 0
_last_warning_time = 0.0
_WARNING_INTERVAL_SECONDS = 300


def _get_mlflow():
    """Lazy-import mlflow. Returns the module or None if not installed."""
    global _mlflow, _mlflow_checked
    if not _mlflow_checked:
        _mlflow_checked = True
        try:
            import mlflow as _m

            _mlflow = _m
        except ImportError:
            _mlflow = None
    return _mlflow


def _truncate_for_tag(value: str) -> str:
    """Truncate a string to fit within MLflow's tag size limit."""
    if len(value) > MLFLOW_TAG_TRUNCATION_LIMIT:
        return value[:MLFLOW_TAG_TRUNCATION_SLICE] + "..."
    return value


def _report_failure(msg: str, exc: Exception) -> None:
    """Rate-limited warning for persistent MLflow logging failures.

    Logs at warning level on the first failure, then only once per
    _WARNING_INTERVAL_SECONDS to avoid flooding logs in production.
    """
    global _consecutive_failures, _last_warning_time
    _consecutive_failures += 1
    now = time.monotonic()
    if _consecutive_failures == 1 or (now - _last_warning_time) >= _WARNING_INTERVAL_SECONDS:
        _logger.warning(
            "%s (failures=%d): %s", msg, _consecutive_failures, exc
        )
        _last_warning_time = now
    else:
        _logger.debug("%s (failures=%d): %s", msg, _consecutive_failures, exc)


def _report_success() -> None:
    """Reset failure counter on successful logging."""
    global _consecutive_failures
    _consecutive_failures = 0


def log_feature_retrieval_to_mlflow(
    feature_refs: List[str],
    entity_count: int,
    duration_seconds: float,
    retrieval_type: str = "historical",
    feature_service: Optional["FeatureService"] = None,
    feature_service_name: Optional[str] = None,
    project: Optional[str] = None,
    tracking_uri: Optional[str] = None,
) -> bool:
    """Log feature retrieval metadata to the active MLflow run.

    This function is a no-op when:
    - mlflow is not installed
    - no MLflow run is currently active
    """
    mlflow = _get_mlflow()
    if mlflow is None:
        return False

    active_run = mlflow.active_run()
    if active_run is None:
        return False

    try:
        client = mlflow.MlflowClient(tracking_uri=tracking_uri)
        run_id = active_run.info.run_id

        if project:
            client.set_tag(run_id, "feast.project", project)
        client.set_tag(run_id, "feast.retrieval_type", retrieval_type)

        fs_name = None
        if feature_service is not None:
            fs_name = feature_service.name
        elif feature_service_name is not None:
            fs_name = feature_service_name
        if fs_name:
            client.set_tag(run_id, "feast.feature_service", fs_name)

        fv_names = sorted({ref.split(":")[0] for ref in feature_refs if ":" in ref})
        if fv_names:
            fv_str = _truncate_for_tag(",".join(fv_names))
            client.set_tag(run_id, "feast.feature_views", fv_str)

        refs_str = _truncate_for_tag(",".join(feature_refs))
        client.set_tag(run_id, "feast.feature_refs", refs_str)
        client.set_tag(run_id, "feast.entity_count", str(entity_count))
        client.set_tag(run_id, "feast.feature_count", str(len(feature_refs)))

        client.log_metric(
            run_id, "feast.job_submission_sec", round(duration_seconds, 4)
        )

        _report_success()
        return True
    except Exception as e:
        _report_failure("Failed to log feature retrieval to MLflow", e)
        return False


def log_training_dataset_to_mlflow(
    df: pd.DataFrame,
    dataset_name: str = "feast_training_data",
    source: Optional[str] = None,
) -> bool:
    """Log a training DataFrame as an MLflow dataset input on the active run.

    This enables dataset versioning: each MLflow run records exactly which
    training data was used.
    """
    mlflow = _get_mlflow()
    if mlflow is None:
        return False

    active_run = mlflow.active_run()
    if active_run is None:
        return False

    try:
        dataset = mlflow.data.from_pandas(
            df,
            name=dataset_name,
            source=source or "feast.get_historical_features",
        )
        mlflow.log_input(dataset, context="training")
        return True
    except Exception as e:
        _report_failure("Failed to log training dataset to MLflow", e)
        return False
