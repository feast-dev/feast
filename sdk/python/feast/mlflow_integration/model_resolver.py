from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Optional

from feast.mlflow_integration.config import resolve_tracking_uri

if TYPE_CHECKING:
    from feast import FeatureStore

_logger = logging.getLogger(__name__)


class FeastMlflowModelResolutionError(Exception):
    """Raised when a model URI cannot be resolved to a feature service."""

    pass


def resolve_feature_service_from_model_uri(
    model_uri: str,
    store: Optional["FeatureStore"] = None,
    tracking_uri: Optional[str] = None,
) -> str:
    """Resolve the Feast feature service name for a given MLflow model URI.

    Resolution order:
      1. Model version tag ``feast.feature_service`` (explicit override).
      2. Training run tag ``feast.feature_service`` (set by auto-log).

    Args:
        model_uri: MLflow model URI in the form ``models:/<name>/<version_or_alias>``.
        store: Optional FeatureStore instance for validating the resolved
            feature service exists and its features match.
        tracking_uri: Optional MLflow tracking URI. When not provided,
            falls back to the MLFLOW_TRACKING_URI environment variable,
            then to MLflow's built-in default.

    Raises:
        FeastMlflowModelResolutionError: If mlflow is not installed, URI is
            invalid, resolution fails, or validation against the store fails.
    """
    try:
        import mlflow
        from mlflow.exceptions import MlflowException
    except ImportError:
        raise FeastMlflowModelResolutionError(
            "mlflow is not installed. Install with: pip install feast[mlflow]"
        )

    pattern = r"^models:/([^/]+)/(.+)$"
    match = re.match(pattern, model_uri)
    if not match:
        raise FeastMlflowModelResolutionError(
            f"Invalid model_uri format: '{model_uri}'. "
            f"Expected 'models:/<model_name>/<version_or_alias>'."
        )

    model_name, version_or_alias = match.group(1), match.group(2)
    effective_uri = resolve_tracking_uri(tracking_uri)
    client = mlflow.MlflowClient(tracking_uri=effective_uri)

    try:
        if version_or_alias.isdigit():
            mv = client.get_model_version(model_name, version_or_alias)
        else:
            mv = client.get_model_version_by_alias(model_name, version_or_alias)
    except MlflowException as e:
        raise FeastMlflowModelResolutionError(
            f"Could not resolve model '{model_uri}': {e}"
        )

    tags = mv.tags or {}
    if "feast.feature_service" in tags:
        fs_name = tags["feast.feature_service"]
    else:
        fs_name = _resolve_from_run_tags(client, mv)
        if fs_name is None:
            raise FeastMlflowModelResolutionError(
                f"Could not determine feature service for model '{model_uri}'. "
                f"No 'feast.feature_service' tag found on the model version or "
                f"its training run. Set the tag explicitly on the model version "
                f"or ensure auto_log was enabled during training."
            )

    if store is not None:
        _validate_feature_service(store, fs_name, client, mv)

    return fs_name


def _resolve_from_run_tags(client, model_version) -> Optional[str]:
    """Check the training run's tags for feast.feature_service."""
    try:
        run = client.get_run(model_version.run_id)
        return run.data.tags.get("feast.feature_service")
    except Exception as e:
        _logger.debug("Could not read run tags for model version: %s", e)
        return None


def _validate_feature_service(store, fs_name, client, model_version):
    """Validate the feature service exists and features match if artifact present."""
    try:
        fs = store.get_feature_service(fs_name)
    except Exception:
        raise FeastMlflowModelResolutionError(
            f"Feature service '{fs_name}' not found in the Feast registry."
        )

    if not _has_artifact(client, model_version.run_id, "required_features.json"):
        return

    try:
        local_path = client.download_artifacts(
            model_version.run_id, "required_features.json"
        )
        with open(local_path) as f:
            expected_features = json.load(f)

        actual_features = []
        for proj in fs.feature_view_projections:
            for feat in proj.features:
                actual_features.append(f"{proj.name}:{feat.name}")

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


def _has_artifact(client, run_id: str, artifact_name: str) -> bool:
    """Check if an artifact exists without triggering a download error."""
    try:
        artifacts = client.list_artifacts(run_id)
        return any(a.path == artifact_name for a in artifacts)
    except Exception:
        return False
