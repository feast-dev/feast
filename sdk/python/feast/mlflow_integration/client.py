from __future__ import annotations

import json
import logging
import os
import re
import tempfile
from typing import TYPE_CHECKING, Any, Dict, List, Optional

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
    """Single integration client for all Feast–MLflow functionality.

    Composes :class:`FeastMlflowLogger`, :class:`FeastMlflowEntityDfBuilder`,
    and :class:`FeastMlflowModelResolver` so that there is exactly **one**
    ``mlflow`` import and **one** ``MlflowClient`` instance.

    Access via ``store.mlflow`` or ``feast.mlflow``::

        store = FeatureStore(".")

        with store.mlflow.start_run(run_name="training"):
            df = store.get_historical_features(...).to_df()
            model = train(df)
            store.mlflow.log_model(model, "model")
    """

    def __init__(self, store: "FeatureStore"):
        import mlflow as _mlflow_mod

        self._mlflow = _mlflow_mod
        self._store = store
        self._tracking_uri = store.config.mlflow.get_tracking_uri()
        self._client = _mlflow_mod.MlflowClient(tracking_uri=self._tracking_uri)
        self._default_experiment = store.config.project

        if self._tracking_uri:
            _mlflow_mod.set_tracking_uri(self._tracking_uri)

        from feast.mlflow_integration.entity_df_builder import (
            FeastMlflowEntityDfBuilder,
        )
        from feast.mlflow_integration.logger import FeastMlflowLogger
        from feast.mlflow_integration.model_resolver import FeastMlflowModelResolver

        self._logger_impl = FeastMlflowLogger(store, _mlflow_mod, self._client)
        self._entity_df_builder = FeastMlflowEntityDfBuilder(
            store, _mlflow_mod, self._client
        )
        self._model_resolver = FeastMlflowModelResolver(
            store, _mlflow_mod, self._client
        )

    @property
    def client(self):
        """The underlying ``MlflowClient`` instance."""
        return self._client

    @property
    def mlflow(self):
        """Escape hatch: access the raw ``mlflow`` module."""
        return self._mlflow

    @property
    def active_run_id(self) -> Optional[str]:
        """Return the active MLflow run ID, or ``None``."""
        run = self._mlflow.active_run()
        return run.info.run_id if run else None

    # ------------------------------------------------------------------
    # Run management
    # ------------------------------------------------------------------

    def start_run(
        self,
        run_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ):
        """Context manager that starts an MLflow run pre-tagged with Feast metadata.

        Sets the default Feast experiment only when a run is actually started,
        avoiding global side effects during ``FeatureStore.__init__``.  If the
        caller already set an experiment (via ``kwargs["experiment_id"]`` or a
        prior ``mlflow.set_experiment``), that choice is respected.
        """
        if "experiment_id" not in kwargs:
            self._mlflow.set_experiment(self._default_experiment)

        merged_tags = {"feast.project": self._store.project}
        if tags:
            merged_tags.update(tags)
        return self._mlflow.start_run(run_name=run_name, tags=merged_tags, **kwargs)

    # ------------------------------------------------------------------
    # Model lifecycle
    # ------------------------------------------------------------------

    def log_model(
        self,
        model: Any,
        artifact_path: str,
        flavor: str = "sklearn",
        **kwargs: Any,
    ):
        """Log a model and auto-attach ``feast_features.json``."""
        flavor_name = _FLAVOR_MAP.get(flavor, "pyfunc")
        flavor_mod = getattr(self._mlflow, flavor_name, self._mlflow.pyfunc)
        flavor_mod.log_model(model, artifact_path, **kwargs)
        self._log_required_features()

    def _log_required_features(self) -> None:
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

            with tempfile.TemporaryDirectory() as tmp_dir:
                path = os.path.join(tmp_dir, "feast_features.json")
                with open(path, "w") as f:
                    json.dump(features, f)
                self._client.log_artifact(run.info.run_id, path, artifact_path="")
        except Exception as e:
            _logger.debug("Failed to log feast_features.json: %s", e)

    def register_model(self, model_uri: str, name: str):
        """Register a model and auto-tag the version with ``feast.feature_service``."""
        result = self._mlflow.register_model(model_uri, name)

        try:
            if result.run_id:
                run = self._client.get_run(result.run_id)
                fs_name = run.data.tags.get("feast.feature_service")
                if fs_name:
                    self._client.set_model_version_tag(
                        name, result.version, "feast.feature_service", fs_name
                    )
        except Exception as e:
            _logger.debug("Failed to auto-tag model version: %s", e)

        return result

    def load_model(self, model_uri: str, **kwargs: Any):
        """Load a model and auto-tag the active prediction run with training lineage."""
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

            self._client.set_tag(run_id, "feast.model_name", model_name)
            self._client.set_tag(run_id, "feast.model_version", str(mv.version))

            if mv.run_id:
                self._client.set_tag(run_id, "feast.training_run_id", mv.run_id)
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

    # ------------------------------------------------------------------
    # Delegated to logger
    # ------------------------------------------------------------------

    def log_feature_retrieval(
        self,
        feature_refs: List[str],
        entity_count: int,
        duration_seconds: float,
        retrieval_type: str = "historical",
        feature_service: Optional[Any] = None,
        feature_service_name: Optional[str] = None,
    ) -> bool:
        """Log feature retrieval metadata to the active MLflow run."""
        return self._logger_impl.log_feature_retrieval(
            feature_refs=feature_refs,
            entity_count=entity_count,
            duration_seconds=duration_seconds,
            retrieval_type=retrieval_type,
            feature_service=feature_service,
            feature_service_name=feature_service_name,
        )

    def log_training_dataset(
        self,
        df: "pd.DataFrame",
        dataset_name: str = "feast_training_data",
        source: Optional[str] = None,
    ) -> bool:
        """Log a training DataFrame as an MLflow dataset input."""
        return self._logger_impl.log_training_dataset(
            df=df, dataset_name=dataset_name, source=source
        )

    def log_apply(
        self,
        changed_objects: List[Any],
        transition_types: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Log a feast apply operation to MLflow."""
        return self._logger_impl.log_apply(
            changed_objects=changed_objects,
            transition_types=transition_types,
        )

    def log_materialize(
        self,
        feature_view_names: List[str],
        start_date: Any,
        end_date: Any,
        duration_seconds: float,
        incremental: bool = False,
    ) -> bool:
        """Log a feast materialize operation to MLflow."""
        return self._logger_impl.log_materialize(
            feature_view_names=feature_view_names,
            start_date=start_date,
            end_date=end_date,
            duration_seconds=duration_seconds,
            incremental=incremental,
        )

    def log_entity_df_metadata(
        self, entity_df: Any, start_date: Any = None, end_date: Any = None
    ) -> None:
        """Log lightweight entity_df metadata to MLflow."""
        self._logger_impl.log_entity_df_metadata(entity_df, start_date, end_date)

    def log_entity_df_artifact(self, entity_df: Any) -> None:
        """Upload entity DataFrame as a parquet artifact to MLflow."""
        self._logger_impl.log_entity_df_artifact(entity_df)

    # ------------------------------------------------------------------
    # Delegated to model resolver
    # ------------------------------------------------------------------

    def resolve_features(self, model_uri: str) -> str:
        """Resolve which Feast feature service a registered model needs."""
        return self._model_resolver.resolve(model_uri)

    # ------------------------------------------------------------------
    # Delegated to entity df builder
    # ------------------------------------------------------------------

    def get_training_entity_df(
        self,
        run_id: str,
        timestamp_column: str = "event_timestamp",
        max_rows: Optional[int] = None,
    ) -> "pd.DataFrame":
        """Pull the entity DataFrame from a past MLflow run."""
        return self._entity_df_builder.get_entity_df(
            run_id=run_id,
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
