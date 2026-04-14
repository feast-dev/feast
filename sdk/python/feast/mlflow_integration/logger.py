from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

import pandas as pd

if TYPE_CHECKING:
    from feast.feature_service import FeatureService

_logger = logging.getLogger(__name__)

_mlflow = None
_mlflow_checked = False


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
            fv_str = ",".join(fv_names)
            if len(fv_str) > 4990:
                fv_str = fv_str[:4987] + "..."
            client.set_tag(run_id, "feast.feature_views", fv_str)

        refs_str = ",".join(feature_refs)
        if len(refs_str) > 4990:
            refs_str = refs_str[:4987] + "..."
        client.set_tag(run_id, "feast.feature_refs", refs_str)
        client.set_tag(run_id, "feast.entity_count", str(entity_count))
        client.set_tag(run_id, "feast.feature_count", str(len(feature_refs)))

        client.log_metric(
            run_id, "feast.job_submission_sec", round(duration_seconds, 4)
        )

        return True
    except Exception as e:
        _logger.warning("Failed to log feature retrieval to MLflow: %s", e)
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
        _logger.warning("Failed to log training dataset to MLflow: %s", e)
        return False
