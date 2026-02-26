# Copyright 2026 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from typing import Any, List, Optional

try:
    import mlflow  # noqa: F401

    MLFLOW_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    MLFLOW_AVAILABLE = False
    mlflow = None  # type: ignore[assignment]


def _require_mlflow() -> None:
    if not MLFLOW_AVAILABLE:
        from feast.errors import FeastExtrasDependencyImportError

        raise FeastExtrasDependencyImportError(
            "mlflow",
            "MLflow is not installed. The Feast–MLflow integration requires the mlflow package.",
        )


def is_mlflow_available() -> bool:
    return MLFLOW_AVAILABLE


def log_feature_retrieval_to_mlflow(
    feature_refs: List[str],
    entity_count: int,
    duration_seconds: float,
    *,
    feature_service_name: Optional[str] = None,
    retrieval_type: Optional[str] = None,
) -> None:

    if not MLFLOW_AVAILABLE or mlflow is None:
        return
    try:
        run = mlflow.active_run()
        if run is None:
            return
    except Exception:
        return
    try:
        if feature_service_name is not None:
            mlflow.log_param("feast.feature_service", feature_service_name)
        mlflow.log_param("feast.feature_refs", json.dumps(feature_refs))
        mlflow.log_param("feast.entity_count", entity_count)
        mlflow.log_param("feast.retrieval_duration_sec", duration_seconds)
        if retrieval_type is not None:
            mlflow.log_param("feast.retrieval_type", retrieval_type)
    except Exception:
        pass


def log_model_with_feast_context(
    *args: Any,
    **kwargs: Any,
) -> Optional[Any]:

    _require_mlflow()
    return None


__all__ = [
    "MLFLOW_AVAILABLE",
    "is_mlflow_available",
    "log_feature_retrieval_to_mlflow",
    "log_model_with_feast_context",
]
