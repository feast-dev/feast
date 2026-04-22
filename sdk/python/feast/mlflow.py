"""
``feast.mlflow`` — drop-in replacement for ``import mlflow`` with Feast superpowers.

Any function or attribute available on the ``mlflow`` module can be accessed
via ``feast.mlflow.*``.  A subset of calls are **Feast-enhanced** with
automatic tagging, lineage tracking, and feature resolution:

- ``start_run()`` — auto-tags runs with ``feast.project``
- ``log_model()`` — auto-saves ``required_features.json``
- ``register_model()`` — auto-tags model versions with ``feast.feature_service``
- ``load_model()`` — auto-links prediction runs to training runs
- ``resolve_features()`` — Feast-only: model URI → feature service name
- ``get_training_entity_df()`` — Feast-only: recover training entity data

All other calls (``log_params``, ``log_metrics``, ``set_tag``,
``log_artifact``, ``MlflowClient``, etc.) pass through to the raw
``mlflow`` module unchanged.

**Store resolution order** (first match wins):

1. Explicit ``feast.mlflow.init(store)`` call
2. Most recently created ``FeatureStore`` (auto-registered)
3. ``FeatureStore(".")`` from the current working directory
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from feast import FeatureStore

_logger = logging.getLogger(__name__)

_client: Optional[Any] = None
_registered_store: Optional["FeatureStore"] = None


def _register_store(store: "FeatureStore") -> None:
    """Called by ``FeatureStore.__init__`` to auto-register itself.

    This is an internal API — end users should call :func:`init` instead.
    """
    global _registered_store
    _registered_store = store


def _build_client() -> Any:
    """Create a ``FeastMlflowClient`` using the best available store.

    Raises a clear error on each failure mode instead of letting raw
    exceptions propagate.
    """
    from feast.mlflow_integration.client import FeastMlflowClient

    store = _registered_store
    if store is None:
        try:
            from feast import FeatureStore

            store = FeatureStore(".")
        except Exception as exc:
            raise RuntimeError(
                "feast.mlflow could not auto-discover a FeatureStore. "
                "Either call feast.mlflow.init(store), create a FeatureStore "
                "before using feast.mlflow, or ensure feature_store.yaml "
                "exists in the current directory."
            ) from exc

    mlflow_cfg = getattr(store.config, "mlflow", None)
    if mlflow_cfg is None or not mlflow_cfg.enabled:
        raise RuntimeError(
            "MLflow integration is not enabled. "
            "Set mlflow.enabled=true in feature_store.yaml, or call "
            "feast.mlflow.init(store) with a store whose config has "
            "mlflow.enabled=true."
        )

    try:
        return FeastMlflowClient(store)
    except ImportError:
        raise ImportError(
            "mlflow package is not installed. "
            "Install it with: pip install feast[mlflow]"
        )


def _ensure_client() -> Any:
    """Return the cached client, creating it on first call."""
    global _client
    if _client is None:
        _client = _build_client()
    return _client


def init(store: "FeatureStore") -> None:
    """Bind ``feast.mlflow`` to a specific :class:`~feast.FeatureStore`.

    Call this once at the start of a notebook or script.  All subsequent
    ``feast.mlflow.*`` calls will use this store's MLflow configuration.

    This is **optional** — if you skip it, ``feast.mlflow`` will use the
    most recently created ``FeatureStore`` automatically.
    """
    global _client, _registered_store
    _client = None
    _registered_store = store


def get_active_run_id() -> Optional[str]:
    """Return the active MLflow run ID, or ``None``."""
    return _ensure_client().active_run_id


def __getattr__(name: str) -> Any:
    """Open delegation: Feast-enhanced client first, raw mlflow fallback.

    Lookup order for ``feast.mlflow.<name>``:

    1. If ``FeastMlflowClient`` has a public attribute *name*, return it.
       This gives Feast-enhanced versions of ``start_run``, ``log_model``,
       ``register_model``, ``load_model``, etc.
    2. Otherwise, fall back to the raw ``mlflow`` module.  This makes
       ``feast.mlflow.log_params``, ``feast.mlflow.set_tag``,
       ``feast.mlflow.MlflowClient``, etc. work without any wrappers.
    """
    if name.startswith("_"):
        raise AttributeError(f"module 'feast.mlflow' has no attribute {name!r}")

    client = _ensure_client()

    client_attr = getattr(client, name, None)
    if client_attr is not None:
        return client_attr

    mlflow_attr = getattr(client._mlflow, name, None)
    if mlflow_attr is not None:
        return mlflow_attr

    raise AttributeError(f"module 'feast.mlflow' has no attribute {name!r}")
