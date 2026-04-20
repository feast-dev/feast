from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    import pandas as pd

    from feast import FeatureStore

_logger = logging.getLogger(__name__)

_FLAVOR_MAP = {
    "sklearn": "sklearn",
    "pytorch": "pytorch",
    "xgboost": "xgboost",
    "lightgbm": "lightgbm",
    "tensorflow": "tensorflow",
    "keras": "keras",
    "pyfunc": "pyfunc",
}


class FeastMlflowClient:
    """Thin wrapper around MLflow that eliminates direct ``import mlflow``.

    All configuration (tracking URI, experiment) is inherited from the
    ``FeatureStore`` instance.  The wrapper auto-tags runs with Feast
    metadata and provides training-to-prediction run linkage via
    :meth:`load_model`.

    Usage::

        store = FeatureStore(".")
        client = store.get_mlflow_client()

        with client.start_run(run_name="training"):
            df = store.get_historical_features(...).to_df()
            model = train(df)
            client.log_model(model, "model")

    """

    def __init__(self, store: "FeatureStore"):
        mlflow_cfg = store.config.mlflow
        if mlflow_cfg is None or not mlflow_cfg.enabled:
            raise ValueError(
                "MLflow integration is not enabled. "
                "Set mlflow.enabled=true in feature_store.yaml."
            )

        try:
            import mlflow as _mlflow_mod
        except ImportError:
            raise ImportError(
                "mlflow is not installed. Install with: pip install feast[mlflow]"
            )

        self._mlflow = _mlflow_mod
        self._store = store
        self._tracking_uri = mlflow_cfg.get_tracking_uri()
        self._client = _mlflow_mod.MlflowClient(tracking_uri=self._tracking_uri)

        if self._tracking_uri:
            _mlflow_mod.set_tracking_uri(self._tracking_uri)
        _mlflow_mod.set_experiment(store.config.project)

    @property
    def mlflow(self):
        """Escape hatch: access the raw ``mlflow`` module."""
        return self._mlflow

    @property
    def active_run_id(self) -> Optional[str]:
        """Return the active MLflow run ID, or ``None``."""
        run = self._mlflow.active_run()
        return run.info.run_id if run else None

    def start_run(
        self,
        run_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs,
    ):
        """Context manager that starts an MLflow run pre-tagged with Feast metadata.

        The ``feast.project`` tag is always set.  Additional tags can be
        passed via *tags*.
        """
        merged_tags = {"feast.project": self._store.project}
        if tags:
            merged_tags.update(tags)
        return self._mlflow.start_run(run_name=run_name, tags=merged_tags, **kwargs)

    def log_params(self, params: Dict[str, Any]):
        """Log parameters to the active run."""
        self._mlflow.log_params(params)

    def log_metrics(self, metrics: Dict[str, float], step: Optional[int] = None):
        """Log metrics to the active run."""
        self._mlflow.log_metrics(metrics, step=step)

    def log_metric(self, key: str, value: float, step: Optional[int] = None):
        """Log a single metric to the active run."""
        self._mlflow.log_metric(key, value, step=step)

    def log_model(
        self,
        model: Any,
        artifact_path: str,
        flavor: str = "sklearn",
        **kwargs,
    ):
        """Log a model and auto-attach ``required_features.json``.

        Supported flavors: sklearn, pytorch, xgboost, lightgbm,
        tensorflow, keras, pyfunc.  Unknown flavors fall back to pyfunc.
        """
        flavor_name = _FLAVOR_MAP.get(flavor, "pyfunc")
        flavor_mod = getattr(self._mlflow, flavor_name, self._mlflow.pyfunc)
        flavor_mod.log_model(model, artifact_path, **kwargs)

        self._log_required_features()

    def _log_required_features(self):
        """If a retrieval happened in this session, log the feature list."""
        try:
            run = self._mlflow.active_run()
            if run is None:
                return
            tags = self._client.get_run(run.info.run_id).data.tags
            refs_str = tags.get("feast.feature_refs")
            if not refs_str:
                return
            features = [r for r in refs_str.split(",") if r]
            if not features:
                return

            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", delete=False
            ) as f:
                json.dump(features, f)
                tmppath = f.name
            try:
                self._mlflow.log_artifact(tmppath, "")
                final_name = os.path.basename(tmppath)
                if final_name != "required_features.json":
                    pass
            finally:
                os.unlink(tmppath)

            fd, tmppath2 = tempfile.mkstemp(suffix=".json")
            try:
                with os.fdopen(fd, "w") as f2:
                    json.dump(features, f2)
                self._client.log_artifact(run.info.run_id, tmppath2, artifact_path="")
            finally:
                if os.path.exists(tmppath2):
                    os.unlink(tmppath2)
        except Exception as e:
            _logger.debug("Failed to log required_features.json: %s", e)

    def register_model(self, model_uri: str, name: str):
        """Register a model and auto-tag the version with ``feast.feature_service``.

        Returns the ``ModelVersion`` object.
        """
        result = self._mlflow.register_model(model_uri, name)

        try:
            run = self._client.get_run(result.run_id)
            fs_name = run.data.tags.get("feast.feature_service")
            if fs_name:
                self._client.set_model_version_tag(
                    name, result.version, "feast.feature_service", fs_name
                )
        except Exception as e:
            _logger.debug("Failed to auto-tag model version: %s", e)

        return result

    def load_model(self, model_uri: str, **kwargs):
        """Load a model and auto-tag the active prediction run with training lineage.

        When called inside an active ``start_run()`` context, this sets:
        - ``feast.training_run_id`` -- the run that produced the model
        - ``feast.model_name`` -- registered model name
        - ``feast.model_version`` -- model version number
        - ``feast.feature_service`` -- copied from the training run

        This creates an explicit bidirectional link between training and
        prediction runs.
        """
        model = self._mlflow.pyfunc.load_model(model_uri, **kwargs)

        try:
            active = self._mlflow.active_run()
            if active is None:
                return model

            run_id = active.info.run_id
            parsed = _parse_model_uri(model_uri)
            if parsed is None:
                return model

            model_name, version_or_alias = parsed
            try:
                if version_or_alias.isdigit():
                    mv = self._client.get_model_version(model_name, version_or_alias)
                else:
                    mv = self._client.get_model_version_by_alias(
                        model_name, version_or_alias
                    )
            except Exception:
                return model

            if mv.run_id:
                self._client.set_tag(run_id, "feast.training_run_id", mv.run_id)
            self._client.set_tag(run_id, "feast.model_name", model_name)
            self._client.set_tag(run_id, "feast.model_version", str(mv.version))

            try:
                training_run = self._client.get_run(mv.run_id)
                fs_name = training_run.data.tags.get("feast.feature_service")
                if fs_name:
                    self._client.set_tag(run_id, "feast.feature_service", fs_name)
            except Exception:
                pass

        except Exception as e:
            _logger.debug("Failed to tag prediction run with training lineage: %s", e)

        return model

    def resolve_features(self, model_uri: str) -> str:
        """Resolve which Feast feature service a registered model needs."""
        from feast.mlflow_integration.model_resolver import (
            resolve_feature_service_from_model_uri,
        )

        return resolve_feature_service_from_model_uri(
            model_uri, store=self._store, tracking_uri=self._tracking_uri
        )

    def get_training_entity_df(
        self,
        run_id: str,
        timestamp_column: str = "event_timestamp",
        max_rows: Optional[int] = None,
    ) -> "pd.DataFrame":
        """Pull the entity DataFrame from a past MLflow run."""
        from feast.mlflow_integration.entity_df_builder import (
            get_entity_df_from_mlflow_run,
        )

        return get_entity_df_from_mlflow_run(
            run_id=run_id,
            tracking_uri=self._tracking_uri,
            timestamp_column=timestamp_column,
            max_rows=max_rows,
        )


def _parse_model_uri(model_uri: str) -> Optional[tuple]:
    """Parse ``models:/<name>/<version_or_alias>`` into a tuple."""
    pattern = r"^models:/([^/]+)/(.+)$"
    match = re.match(pattern, model_uri)
    if match:
        return match.group(1), match.group(2)
    return None
