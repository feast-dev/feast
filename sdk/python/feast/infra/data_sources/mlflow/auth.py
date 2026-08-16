"""Thread-safe token resolution for MLflow API calls from Feast DataSources.

Token resolution chain (first non-empty wins):
  1. ``SecurityManager.current_request_token`` — user-initiated request token
  2. ``MLFLOW_TRACKING_TOKEN`` env var — explicit token override
  3. ServiceAccount token at ``/var/run/secrets/kubernetes.io/serviceaccount/token``
  4. ``None`` — no auth (local dev, anonymous access)
"""

from __future__ import annotations

import contextlib
import logging
import os
import threading
from contextvars import ContextVar
from pathlib import Path
from typing import Dict, Iterator, Optional

logger = logging.getLogger(__name__)

_SA_TOKEN_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")

_current_mlflow_token: ContextVar[Optional[str]] = ContextVar(
    "feast_mlflow_token", default=None
)

_tracking_uri_lock = threading.RLock()


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
    """Activate *token* for the current async/thread context.

    The ``FeastMLflowHeaderProvider`` (registered via entry-point) reads
    this ``ContextVar`` and injects the ``Authorization`` header into every
    outgoing MLflow REST request — no ``os.environ`` mutation required.
    """
    reset = _current_mlflow_token.set(token)
    try:
        yield
    finally:
        _current_mlflow_token.reset(reset)


@contextlib.contextmanager
def mlflow_tracking_scope(tracking_uri: Optional[str]) -> Iterator[None]:
    """Thread-safe scope for ``mlflow.set_tracking_uri()``.

    ``mlflow.set_tracking_uri()`` mutates process-global state, so
    concurrent calls with different URIs would race.  This context manager
    serialises access behind an RLock, restoring the previous URI on exit.

    Fast paths (no lock acquired):
      - *tracking_uri* is ``None``
      - *tracking_uri* matches the current global URI
    """
    if tracking_uri is None:
        yield
        return

    import mlflow

    current = mlflow.get_tracking_uri()
    if tracking_uri == current:
        yield
        return

    with _tracking_uri_lock:
        prev = mlflow.get_tracking_uri()
        try:
            mlflow.set_tracking_uri(tracking_uri)
            yield
        finally:
            mlflow.set_tracking_uri(prev)


@contextlib.contextmanager
def mlflow_request_scope(
    token: Optional[str], tracking_uri: Optional[str]
) -> Iterator[None]:
    """Combined scope: token (via ContextVar) + tracking URI (via lock).

    Nests both scopes so callers get a single context manager for the
    full auth + routing setup.
    """
    with mlflow_token_scope(token), mlflow_tracking_scope(tracking_uri):
        yield


def get_current_mlflow_token() -> Optional[str]:
    """Return the MLflow token active in the current context, if any."""
    return _current_mlflow_token.get()


class FeastMLflowHeaderProvider:
    """Inject Feast-resolved auth tokens into MLflow REST requests.

    Registered as an ``mlflow.request_header_provider`` entry-point plugin.
    MLflow calls ``in_context()`` on every outgoing request; when ``True``,
    it merges the dict returned by ``request_headers()`` into the HTTP
    headers.
    """

    def in_context(self) -> bool:
        return _current_mlflow_token.get() is not None

    def request_headers(self) -> Dict[str, str]:
        token = _current_mlflow_token.get()
        if token:
            return {"Authorization": f"Bearer {token}"}
        return {}
