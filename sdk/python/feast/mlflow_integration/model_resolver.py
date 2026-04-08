from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from feast import FeatureStore

_logger = logging.getLogger(__name__)


class FeastMlflowModelResolutionError(Exception):
    """Raised when a model URI cannot be resolved to a feature service."""

    pass


def resolve_feature_service_from_model_uri(
    model_uri: str,
    store: Optional["FeatureStore"] = None,
) -> str:
    """Resolve the Feast feature service name for a given MLflow model URI.

    Resolution order:
      1. Model tag ``feast.feature_service`` (explicit override).
      2. Naming convention: ``{model_name}_v{version}``.

    Args:
        model_uri: MLflow model URI, e.g. ``models:/fraud-model/Production``
            or ``models:/fraud-model/1``.
        store: Optional FeatureStore instance.  When provided the resolved
            feature service is validated against the registry, and if the
            model has an artifact ``required_features.json`` the feature
            list is checked for consistency.

    Returns:
        Feature service name string.

    Raises:
        FeastMlflowModelResolutionError: If mlflow is not installed, URI is
            invalid, or validation against the store fails.
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
    client = mlflow.MlflowClient()

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
        fs_name = f"{model_name}_v{mv.version}"

    if store is not None:
        _validate_feature_service(store, fs_name, client, mv)

    return fs_name


def _validate_feature_service(store, fs_name, client, model_version):
    """Validate the feature service exists and features match if artifact present."""
    try:
        fs = store.get_feature_service(fs_name)
    except Exception:
        raise FeastMlflowModelResolutionError(
            f"Feature service '{fs_name}' not found in the Feast registry."
        )

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
    except Exception:
        # No artifact or download failed — skip validation silently
        pass
