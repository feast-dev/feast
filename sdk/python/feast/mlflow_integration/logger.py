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
    from feast import FeatureStore
    from feast.feature_service import FeatureService

_logger = logging.getLogger(__name__)

_WARNING_INTERVAL_SECONDS = 300


class FeastMlflowLogger:
    """Handles all MLflow logging for Feast feature retrieval and operations.

    Instantiated once inside :class:`FeastMlflowClient` and reuses its
    ``mlflow`` module reference and ``MlflowClient`` — no duplicate
    ``import mlflow`` or client construction.
    """

    def __init__(self, store: "FeatureStore", mlflow_mod: Any, client: Any):
        self._store = store
        self._mlflow = mlflow_mod
        self._client = client
        self._tracking_uri = store.config.mlflow.get_tracking_uri()
        self._consecutive_failures = 0
        self._last_warning_time = 0.0

    def _truncate_for_tag(self, value: str) -> str:
        if len(value) > MLFLOW_TAG_TRUNCATION_LIMIT:
            return value[:MLFLOW_TAG_TRUNCATION_SLICE] + "..."
        return value

    def _report_failure(self, msg: str, exc: Exception) -> None:
        self._consecutive_failures += 1
        now = time.monotonic()
        if (
            self._consecutive_failures == 1
            or (now - self._last_warning_time) >= _WARNING_INTERVAL_SECONDS
        ):
            _logger.warning(
                "%s (failures=%d): %s", msg, self._consecutive_failures, exc
            )
            self._last_warning_time = now
        else:
            _logger.debug("%s (failures=%d): %s", msg, self._consecutive_failures, exc)

    def _report_success(self) -> None:
        self._consecutive_failures = 0

    def log_feature_retrieval(
        self,
        feature_refs: List[str],
        entity_count: int,
        duration_seconds: float,
        retrieval_type: str = "historical",
        feature_service: Optional["FeatureService"] = None,
        feature_service_name: Optional[str] = None,
    ) -> bool:
        """Log feature retrieval metadata to the active MLflow run."""
        active_run = self._mlflow.active_run()
        if active_run is None:
            return False

        try:
            run_id = active_run.info.run_id

            if self._store.project:
                self._client.set_tag(run_id, "feast.project", self._store.project)
            self._client.set_tag(run_id, "feast.retrieval_type", retrieval_type)

            fs_name = None
            if feature_service is not None:
                fs_name = feature_service.name
            elif feature_service_name is not None:
                fs_name = feature_service_name
            if fs_name:
                self._client.set_tag(run_id, "feast.feature_service", fs_name)

            fv_names = sorted({ref.split(":")[0] for ref in feature_refs if ":" in ref})
            if fv_names:
                fv_str = self._truncate_for_tag(",".join(fv_names))
                self._client.set_tag(run_id, "feast.feature_views", fv_str)

            refs_str = self._truncate_for_tag(",".join(feature_refs))
            self._client.set_tag(run_id, "feast.feature_refs", refs_str)
            self._client.set_tag(run_id, "feast.entity_count", str(entity_count))
            self._client.set_tag(run_id, "feast.feature_count", str(len(feature_refs)))

            self._client.log_metric(
                run_id, "feast.job_submission_sec", round(duration_seconds, 4)
            )

            self._report_success()
            return True
        except Exception as e:
            self._report_failure("Failed to log feature retrieval to MLflow", e)
            return False

    def log_training_dataset(
        self,
        df: pd.DataFrame,
        dataset_name: str = "feast_training_data",
        source: Optional[str] = None,
    ) -> bool:
        """Log a training DataFrame as an MLflow dataset input on the active run."""
        active_run = self._mlflow.active_run()
        if active_run is None:
            return False

        try:
            dataset = self._mlflow.data.from_pandas(
                df,
                name=dataset_name,
                source=source or "feast.get_historical_features",
            )
            self._mlflow.log_input(dataset, context="training")
            return True
        except Exception as e:
            self._report_failure("Failed to log training dataset to MLflow", e)
            return False

    def _get_or_create_experiment(
        self, experiment_name: str, client: Any = None
    ) -> str:
        c = client or self._client
        exp = c.get_experiment_by_name(experiment_name)
        if exp is not None:
            return exp.experiment_id
        return c.create_experiment(experiment_name)

    def log_apply(
        self,
        changed_objects: List[Any],
        transition_types: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Log a feast apply operation to a dedicated MLflow experiment."""
        try:
            from feast import Entity, FeatureService
            from feast.feature_view import FeatureView

            project = self._store.project
            mlflow_cfg = self._store.config.mlflow
            ops_suffix = mlflow_cfg.ops_experiment_suffix

            effective_uri = self._tracking_uri or self._mlflow.get_tracking_uri()
            client = self._mlflow.MlflowClient(tracking_uri=effective_uri)

            experiment_name = f"{project}{ops_suffix}"
            experiment_id = self._get_or_create_experiment(
                experiment_name, client=client
            )

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
                        self._truncate_for_tag(",".join(fv_names)),
                    )
                if fs_names:
                    client.set_tag(
                        run_id,
                        "feast.feature_services_changed",
                        self._truncate_for_tag(",".join(fs_names)),
                    )
                if entity_names:
                    client.set_tag(
                        run_id,
                        "feast.entities_changed",
                        self._truncate_for_tag(",".join(entity_names)),
                    )
                client.log_metric(
                    run_id, "feast.apply.feature_views_count", len(fv_names)
                )
                client.log_metric(
                    run_id, "feast.apply.feature_services_count", len(fs_names)
                )
                client.log_metric(
                    run_id, "feast.apply.entities_count", len(entity_names)
                )

                if transition_types:
                    self._log_transition_tags(
                        client,
                        run_id,
                        transition_types,
                        fv_names,
                        fs_names,
                        entity_names,
                    )
            finally:
                client.set_terminated(run_id)

            self._report_success()
            return True
        except Exception as e:
            self._report_failure("Failed to log apply to MLflow", e)
            return False

    def _log_transition_tags(
        self,
        client: Any,
        run_id: str,
        transition_types: Dict[str, str],
        fv_names: List[str],
        fs_names: List[str],
        entity_names: List[str],
    ) -> None:
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
                        self._truncate_for_tag(",".join(names)),
                    )

    def log_materialize(
        self,
        feature_view_names: List[str],
        start_date: Optional[datetime],
        end_date: datetime,
        duration_seconds: float,
        incremental: bool = False,
    ) -> bool:
        """Log a feast materialize operation to a dedicated MLflow experiment."""
        try:
            project = self._store.project
            mlflow_cfg = self._store.config.mlflow
            ops_suffix = mlflow_cfg.ops_experiment_suffix

            effective_uri = self._tracking_uri or self._mlflow.get_tracking_uri()
            client = self._mlflow.MlflowClient(tracking_uri=effective_uri)

            experiment_name = f"{project}{ops_suffix}"
            experiment_id = self._get_or_create_experiment(
                experiment_name, client=client
            )

            op_type = "materialize_incremental" if incremental else "materialize"
            run = client.create_run(experiment_id, run_name=f"{op_type}_{project}")
            run_id = run.info.run_id
            try:
                client.set_tag(run_id, "feast.operation", op_type)
                client.set_tag(run_id, "feast.project", project)
                client.set_tag(
                    run_id,
                    "feast.materialize.feature_views",
                    self._truncate_for_tag(",".join(feature_view_names)),
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

            self._report_success()
            return True
        except Exception as e:
            self._report_failure("Failed to log materialize to MLflow", e)
            return False

    def log_entity_df_metadata(
        self, entity_df: Any, start_date: Any = None, end_date: Any = None
    ) -> None:
        """Log lightweight entity_df metadata to MLflow.

        Uses ``set_tag`` (not ``log_param``) so the metadata can safely be
        updated when ``get_historical_features`` is called multiple times
        within the same MLflow run.
        """
        try:
            if self._mlflow.active_run() is None:
                return
            run_id = self._mlflow.active_run().info.run_id

            if isinstance(entity_df, str):
                if len(entity_df) > MLFLOW_TAG_TRUNCATION_LIMIT:
                    query = entity_df[:MLFLOW_TAG_TRUNCATION_SLICE] + "..."
                else:
                    query = entity_df
                self._client.set_tag(run_id, "feast.entity_df_query", query)
                self._client.set_tag(run_id, "feast.entity_df_type", "sql")

            elif isinstance(entity_df, pd.DataFrame):
                self._client.set_tag(run_id, "feast.entity_df_type", "dataframe")
                self._client.set_tag(
                    run_id, "feast.entity_df_rows", str(len(entity_df))
                )
                cols = ",".join(entity_df.columns)
                if len(cols) > MLFLOW_TAG_TRUNCATION_LIMIT:
                    cols = cols[:MLFLOW_TAG_TRUNCATION_SLICE] + "..."
                self._client.set_tag(run_id, "feast.entity_df_columns", cols)

            elif entity_df is None and (start_date or end_date):
                self._client.set_tag(run_id, "feast.entity_df_type", "range")
                if start_date:
                    self._client.set_tag(run_id, "feast.start_date", str(start_date))
                if end_date:
                    self._client.set_tag(run_id, "feast.end_date", str(end_date))

        except Exception as e:
            _logger.debug("Failed to log entity_df metadata to MLflow: %s", e)

    def log_entity_df_artifact(self, entity_df: Any) -> None:
        """Upload entity DataFrame as a parquet artifact to MLflow."""
        try:
            import os
            import tempfile

            if self._mlflow.active_run() is None:
                return
            if not isinstance(entity_df, pd.DataFrame):
                return

            mlflow_cfg = self._store.config.mlflow
            run_id = self._mlflow.active_run().info.run_id

            max_rows = mlflow_cfg.entity_df_max_rows
            if len(entity_df) <= max_rows:
                with tempfile.TemporaryDirectory() as tmp_dir:
                    path = os.path.join(tmp_dir, "entity_df.parquet")
                    entity_df.to_parquet(path, index=False)
                    self._client.log_artifact(run_id, path)

        except Exception as e:
            _logger.debug("Failed to log entity_df artifact to MLflow: %s", e)
