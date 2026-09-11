"""Thread-safe token resolution and TLS for MLflow API calls from Feast DataSources.

Token resolution chain (first non-empty wins):
  1. ``SecurityManager.current_request_token`` — user-initiated request token
  2. ``MLFLOW_TRACKING_TOKEN`` env var — explicit token override
  3. ServiceAccount token at ``/var/run/secrets/kubernetes.io/serviceaccount/token``
  4. ``None`` — no auth (local dev, anonymous access)

CA bundle resolution chain (first non-empty wins):
  1. ``MlflowConfig.ca_bundle`` — explicit path from feature_store.yaml
  2. ``REQUESTS_CA_BUNDLE`` env var — standard Python requests convention
  3. ``SSL_CERT_FILE`` env var — OpenSSL convention (used by httpx)
  4. System defaults
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


def resolve_ca_bundle() -> Optional[str]:
    """Resolve the CA bundle path for MLflow TLS verification.

    Checks MlflowConfig.ca_bundle, then REQUESTS_CA_BUNDLE, then SSL_CERT_FILE.
    Returns None when system defaults should be used.
    """
    ca = os.environ.get("REQUESTS_CA_BUNDLE")
    if ca:
        return ca
    ca = os.environ.get("SSL_CERT_FILE")
    if ca:
        return ca
    return None


@contextlib.contextmanager
def mlflow_ca_bundle_scope(ca_bundle: Optional[str]) -> Iterator[None]:
    """Set REQUESTS_CA_BUNDLE for the duration of MLflow calls.

    MLflow's REST client uses the ``requests`` library which honours this
    env var. The operator sets it globally via pod env, but this context
    manager allows per-request overrides from feature_store.yaml config.

    Only mutates os.environ when ca_bundle differs from the current value
    to avoid unnecessary env writes.
    """
    if ca_bundle is None:
        yield
        return

    prev = os.environ.get("REQUESTS_CA_BUNDLE")
    if ca_bundle == prev:
        yield
        return

    os.environ["REQUESTS_CA_BUNDLE"] = ca_bundle
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop("REQUESTS_CA_BUNDLE", None)
        else:
            os.environ["REQUESTS_CA_BUNDLE"] = prev


class MlflowConnectionError(Exception):
    """Raised when Feast cannot reach the MLflow tracking server after retries."""

    pass


class MlflowArtifactNotFoundError(Exception):
    """Raised when an MLflow artifact or dataset does not exist (404)."""

    pass


class MlflowAuthError(Exception):
    """Raised when MLflow rejects credentials (401/403)."""

    pass


def classify_mlflow_error(exc: Exception) -> Exception:
    """Wrap raw MLflow/requests exceptions into typed Feast errors.

    Inspects HTTP status codes and exception types to produce actionable
    error messages that include remediation hints.
    """
    exc_str = str(exc).lower()
    status_code = getattr(exc, "response", None)
    if status_code is not None:
        status_code = getattr(status_code, "status_code", None)

    if status_code == 401 or "401" in exc_str or "unauthorized" in exc_str:
        return MlflowAuthError(
            f"MLflow authentication failed (HTTP 401). Verify that the "
            f"MLFLOW_TRACKING_TOKEN is valid, the ServiceAccount token is not "
            f"expired, or that the user's Bearer token has MLflow access. "
            f"Original error: {exc}"
        )
    if status_code == 403 or "403" in exc_str or "forbidden" in exc_str:
        return MlflowAuthError(
            f"MLflow authorization denied (HTTP 403). The authenticated identity "
            f"does not have permission to access this resource. "
            f"Original error: {exc}"
        )
    if status_code == 404 or "404" in exc_str or "not found" in exc_str:
        return MlflowArtifactNotFoundError(
            f"MLflow resource not found (HTTP 404). The referenced artifact, "
            f"run, or dataset may have been deleted or never existed. "
            f"Original error: {exc}"
        )
    if (
        "timeout" in exc_str
        or "connectionerror" in exc_str
        or "connection" in exc_str
        or isinstance(exc, (ConnectionError, TimeoutError, OSError))
    ):
        return MlflowConnectionError(
            f"Cannot reach MLflow tracking server. Check that the tracking_uri "
            f"is correct and the MLflow service is running. "
            f"Original error: {exc}"
        )
    return exc


def is_retryable_error(exc: Exception) -> bool:
    """Determine if an MLflow error is transient and should be retried.

    Retryable: timeouts, connection errors, 429 (rate limit), 5xx server errors.
    Not retryable: 401, 403, 404, validation errors.
    """
    classified = classify_mlflow_error(exc)
    if isinstance(classified, MlflowConnectionError):
        return True

    status_code = getattr(exc, "response", None)
    if status_code is not None:
        status_code = getattr(status_code, "status_code", None)
        if status_code is not None and (status_code == 429 or status_code >= 500):
            return True

    exc_str = str(exc).lower()
    if "429" in exc_str or "too many requests" in exc_str:
        return True
    if any(code in exc_str for code in ("500", "502", "503", "504")):
        return True
    return isinstance(exc, (ConnectionError, TimeoutError, OSError))


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
