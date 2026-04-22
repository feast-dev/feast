from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

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
    if (
        _consecutive_failures == 1
        or (now - _last_warning_time) >= _WARNING_INTERVAL_SECONDS
    ):
        _logger.warning("%s (failures=%d): %s", msg, _consecutive_failures, exc)
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


def _get_or_create_experiment(client, experiment_name: str) -> str:
    """Return the experiment ID for *experiment_name*, creating it if needed."""
    exp = client.get_experiment_by_name(experiment_name)
    if exp is not None:
        return exp.experiment_id
    return client.create_experiment(experiment_name)


def log_apply_to_mlflow(
    changed_objects: List[Any],
    project: str,
    tracking_uri: Optional[str] = None,
    ops_experiment_suffix: str = "-feast-ops",
    transition_types: Optional[Dict[str, str]] = None,
) -> bool:
    """Log a feast apply operation to a dedicated MLflow experiment.

    Uses ``MlflowClient`` with explicit experiment IDs so that global
    MLflow state (tracking URI, active experiment) is never mutated.

    Args:
        changed_objects: Feast objects that were affected by the apply.
        project: Feast project name.
        tracking_uri: MLflow tracking URI.
        ops_experiment_suffix: Suffix for the ops experiment name.
        transition_types: Optional mapping of object name to transition
            type string (``"CREATE"``, ``"UPDATE"``, ``"DELETE"``).
            When provided, per-transition tags are logged alongside
            the aggregate ``*_changed`` tags.
    """
    mlflow = _get_mlflow()
    if mlflow is None:
        return False

    try:
        from feast import Entity, FeatureService
        from feast.feature_view import FeatureView

        effective_uri = tracking_uri or mlflow.get_tracking_uri()
        client = mlflow.MlflowClient(tracking_uri=effective_uri)

        experiment_name = f"{project}{ops_experiment_suffix}"
        experiment_id = _get_or_create_experiment(client, experiment_name)

        fv_names: List[str] = []
        fs_names: List[str] = []
        entity_names: List[str] = []
        for obj in changed_objects:
            if isinstance(obj, FeatureView):
                fv_names.append(obj.name)
            elif isinstance(obj, FeatureService):
                fs_names.append(obj.name)
            elif isinstance(obj, Entity) and obj.name != "__dummy":
                entity_names.append(obj.name)

        run = client.create_run(experiment_id, run_name=f"apply_{project}")
        run_id = run.info.run_id
        try:
            client.set_tag(run_id, "feast.operation", "apply")
            client.set_tag(run_id, "feast.project", project)
            if fv_names:
                client.set_tag(
                    run_id,
                    "feast.feature_views_changed",
                    _truncate_for_tag(",".join(fv_names)),
                )
            if fs_names:
                client.set_tag(
                    run_id,
                    "feast.feature_services_changed",
                    _truncate_for_tag(",".join(fs_names)),
                )
            if entity_names:
                client.set_tag(
                    run_id,
                    "feast.entities_changed",
                    _truncate_for_tag(",".join(entity_names)),
                )
            client.log_metric(run_id, "feast.apply.feature_views_count", len(fv_names))
            client.log_metric(
                run_id, "feast.apply.feature_services_count", len(fs_names)
            )
            client.log_metric(run_id, "feast.apply.entities_count", len(entity_names))

            if transition_types:
                _log_transition_tags(
                    client,
                    run_id,
                    transition_types,
                    fv_names,
                    fs_names,
                    entity_names,
                )
        finally:
            client.set_terminated(run_id)

        _report_success()
        return True
    except Exception as e:
        _report_failure("Failed to log apply to MLflow", e)
        return False


def _log_transition_tags(
    client: Any,
    run_id: str,
    transition_types: Dict[str, str],
    fv_names: List[str],
    fs_names: List[str],
    entity_names: List[str],
) -> None:
    """Write per-transition-type tags (created/updated/deleted) to the run."""
    buckets: Dict[str, Dict[str, List[str]]] = {
        "feature_views": {"created": [], "updated": [], "deleted": []},
        "feature_services": {"created": [], "updated": [], "deleted": []},
        "entities": {"created": [], "updated": [], "deleted": []},
    }

    for name in fv_names:
        tt = transition_types.get(name, "").upper()
        if tt in ("CREATE", "UPDATE", "DELETE"):
            buckets["feature_views"][tt.lower() + "d"].append(name)

    for name in fs_names:
        tt = transition_types.get(name, "").upper()
        if tt in ("CREATE", "UPDATE", "DELETE"):
            buckets["feature_services"][tt.lower() + "d"].append(name)

    for name in entity_names:
        tt = transition_types.get(name, "").upper()
        if tt in ("CREATE", "UPDATE", "DELETE"):
            buckets["entities"][tt.lower() + "d"].append(name)

    for obj_type, transitions in buckets.items():
        for transition, names in transitions.items():
            if names:
                client.set_tag(
                    run_id,
                    f"feast.{obj_type}_{transition}",
                    _truncate_for_tag(",".join(names)),
                )


def log_materialize_to_mlflow(
    feature_view_names: List[str],
    start_date: Optional[datetime],
    end_date: datetime,
    duration_seconds: float,
    project: str,
    tracking_uri: Optional[str] = None,
    incremental: bool = False,
    ops_experiment_suffix: str = "-feast-ops",
) -> bool:
    """Log a feast materialize operation to a dedicated MLflow experiment.

    Uses ``MlflowClient`` with explicit experiment IDs so that global
    MLflow state (tracking URI, active experiment) is never mutated.
    """
    mlflow = _get_mlflow()
    if mlflow is None:
        return False

    try:
        effective_uri = tracking_uri or mlflow.get_tracking_uri()
        client = mlflow.MlflowClient(tracking_uri=effective_uri)

        experiment_name = f"{project}{ops_experiment_suffix}"
        experiment_id = _get_or_create_experiment(client, experiment_name)

        op_type = "materialize_incremental" if incremental else "materialize"
        run = client.create_run(experiment_id, run_name=f"{op_type}_{project}")
        run_id = run.info.run_id
        try:
            client.set_tag(run_id, "feast.operation", op_type)
            client.set_tag(run_id, "feast.project", project)
            client.set_tag(
                run_id,
                "feast.materialize.feature_views",
                _truncate_for_tag(",".join(feature_view_names)),
            )
            if start_date:
                client.log_param(
                    run_id,
                    "feast.materialize.start_date",
                    start_date.isoformat(),
                )
            client.log_param(
                run_id,
                "feast.materialize.end_date",
                end_date.isoformat(),
            )
            client.log_metric(
                run_id,
                "feast.materialize.duration_sec",
                round(duration_seconds, 4),
            )
        finally:
            client.set_terminated(run_id)

        _report_success()
        return True
    except Exception as e:
        _report_failure("Failed to log materialize to MLflow", e)
        return False
