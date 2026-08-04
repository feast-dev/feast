"""Token resolution for MLflow API calls from Feast DataSources.

Fallback chain (first non-empty wins):
  1. ``SecurityManager.current_request_token`` — user-initiated request token
  2. ``MLFLOW_TRACKING_TOKEN`` env var — explicit token override
  3. ServiceAccount token at ``/var/run/secrets/kubernetes.io/serviceaccount/token``
  4. ``None`` — no auth (local dev, anonymous access)

MLflow does not support per-request auth tokens on ``MlflowClient``.
Auth is configured via the ``MLFLOW_TRACKING_TOKEN`` environment variable.
The :func:`mlflow_token_scope` context manager sets and restores this env
var to provide request-scoped token forwarding.
"""

from __future__ import annotations

import contextlib
import logging
import os
from pathlib import Path
from typing import Iterator, Optional

logger = logging.getLogger(__name__)

_SA_TOKEN_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")


def resolve_mlflow_token() -> Optional[str]:
    """Resolve an MLflow authentication token using the fallback chain."""
    token = _from_request_context()
    if token:
        return token

    token = _from_env()
    if token:
        return token

    token = _from_service_account()
    if token:
        return token

    logger.debug("No MLflow auth token resolved; proceeding without auth")
    return None


def _from_request_context() -> Optional[str]:
    """Retrieve the raw Bearer token from the current request context."""
    try:
        from feast.permissions.security_manager import get_security_manager

        sm = get_security_manager()
        if sm is not None:
            token = sm.current_request_token
            if token:
                logger.debug("Using request-context token for MLflow auth")
                return token
    except Exception:
        pass
    return None


def _from_env() -> Optional[str]:
    """Check the ``MLFLOW_TRACKING_TOKEN`` environment variable."""
    token = os.environ.get("MLFLOW_TRACKING_TOKEN")
    if token:
        logger.debug("Using MLFLOW_TRACKING_TOKEN env var for MLflow auth")
        return token
    return None


def _from_service_account() -> Optional[str]:
    """Read the Kubernetes ServiceAccount token file if present."""
    if _SA_TOKEN_PATH.is_file():
        try:
            token = _SA_TOKEN_PATH.read_text().strip()
            if token:
                logger.debug(
                    "Using ServiceAccount token from %s for MLflow auth",
                    _SA_TOKEN_PATH,
                )
                return token
        except OSError:
            logger.debug(
                "Failed to read SA token from %s", _SA_TOKEN_PATH, exc_info=True
            )
    return None


@contextlib.contextmanager
def mlflow_token_scope(token: Optional[str]) -> Iterator[None]:
    """Temporarily set ``MLFLOW_TRACKING_TOKEN`` for the duration of a block.

    Restores the previous value (or removes the var) when the block exits.
    This is the recommended way to forward per-request tokens since MLflow
    does not support per-client auth tokens.
    """
    env_key = "MLFLOW_TRACKING_TOKEN"
    prev = os.environ.get(env_key)
    try:
        if token:
            os.environ[env_key] = token
        yield
    finally:
        if prev is not None:
            os.environ[env_key] = prev
        elif env_key in os.environ:
            del os.environ[env_key]
