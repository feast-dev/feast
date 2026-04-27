from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from feast import FeatureStore

_logger = logging.getLogger(__name__)


class FeastMlflowModelResolutionError(Exception):
    """Raised when a model URI cannot be resolved to a feature service."""

    pass


class FeastMlflowModelResolver:
    """Resolves MLflow model URIs to Feast feature service names.

    Instantiated once inside :class:`FeastMlflowClient` and reuses its
    ``MlflowClient`` — no separate ``import mlflow`` needed.
    """

    def __init__(self, store: "FeatureStore", mlflow_mod: Any, client: Any):
        self._store = store
        self._mlflow = mlflow_mod
        self._client = client

    def resolve(self, model_uri: str) -> str:
        """Resolve the Feast feature service name for a given MLflow model URI.

        Resolution order:
          1. Model version tag ``feast.feature_service`` (explicit override).
          2. Training run tag ``feast.feature_service`` (set by auto-log).

        Args:
            model_uri: MLflow model URI in the form
                ``models:/<name>/<version_or_alias>``.

        Raises:
            FeastMlflowModelResolutionError: If URI is invalid, resolution
                fails, or validation against the store fails.
        """
        from mlflow.exceptions import MlflowException

        pattern = r"^models:/([^/]+)/(.+)$"
        match = re.match(pattern, model_uri)
        if not match:
            raise FeastMlflowModelResolutionError(
                f"Invalid model_uri format: '{model_uri}'. "
                f"Expected 'models:/<model_name>/<version_or_alias>'."
            )

        model_name, version_or_alias = match.group(1), match.group(2)

        try:
            if version_or_alias.isdigit():
                mv = self._client.get_model_version(model_name, version_or_alias)
            else:
                mv = self._client.get_model_version_by_alias(
                    model_name, version_or_alias
                )
        except MlflowException as e:
            raise FeastMlflowModelResolutionError(
                f"Could not resolve model '{model_uri}': {e}"
            )

        tags = mv.tags or {}
        if "feast.feature_service" in tags:
            fs_name = tags["feast.feature_service"]
        else:
            fs_name = self._resolve_from_run_tags(mv)
            if fs_name is None:
                raise FeastMlflowModelResolutionError(
                    f"Could not determine feature service for model '{model_uri}'. "
                    f"No 'feast.feature_service' tag found on the model version or "
                    f"its training run. Set the tag explicitly on the model version "
                    f"or ensure auto_log was enabled during training."
                )

        self._validate_feature_service(fs_name, mv)
        return fs_name

    def _resolve_from_run_tags(self, model_version: Any) -> Optional[str]:
        try:
            run = self._client.get_run(model_version.run_id)
            return run.data.tags.get("feast.feature_service")
        except Exception as e:
            _logger.debug("Could not read run tags for model version: %s", e)
            return None

    def _validate_feature_service(self, fs_name: str, model_version: Any) -> None:
        try:
            fs = self._store.get_feature_service(fs_name)
        except Exception:
            raise FeastMlflowModelResolutionError(
                f"Feature service '{fs_name}' not found in the Feast registry."
            )

        if not self._has_artifact(model_version.run_id, "required_features.json"):
            return

        try:
            local_path = self._client.download_artifacts(
                model_version.run_id, "required_features.json"
            )
            with open(local_path) as f:
                expected_features = json.load(f)

            actual_features = []
            for proj in fs.feature_view_projections:
                for feat in proj.features:
                    actual_features.append(f"{proj.name_to_use()}:{feat.name}")

            expected_set = set(expected_features)
            actual_set = set(actual_features)

            if expected_set != actual_set:
                missing = expected_set - actual_set
                extra = actual_set - expected_set
                raise FeastMlflowModelResolutionError(
                    f"Feature mismatch for service '{fs_name}'. "
                    f"Missing: {missing}, Extra: {extra}"
                )
        except FeastMlflowModelResolutionError:
            raise
        except Exception as e:
            _logger.debug("Could not validate required_features.json: %s", e)

    def _has_artifact(self, run_id: str, artifact_name: str) -> bool:
        try:
            artifacts = self._client.list_artifacts(run_id)
            return any(a.path == artifact_name for a in artifacts)
        except Exception:
            return False
