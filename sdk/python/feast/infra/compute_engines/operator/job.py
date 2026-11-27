import logging
import time
from typing import Any, Dict, Optional, cast

from kubernetes import client
from kubernetes.client.exceptions import ApiException

from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
)

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 2
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _is_retryable(exc: ApiException) -> bool:
    return exc.status in _RETRYABLE_STATUS_CODES


def _rbac_hint(status: int) -> str:
    if status in (401, 403):
        return (
            " Check that the Feast server ServiceAccount has the required"
            " Role/RoleBinding for the operator's custom resources and configmaps"
            " in this namespace."
        )
    return ""


class OperatorMaterializationJob(MaterializationJob):
    """MaterializationJob that tracks a Kubernetes operator custom resource.

    Shared polling, retry, and terminal-status caching for operator-backed
    engines. Subclasses declare the CR coordinates
    (``_cr_group``/``_cr_version``/``_cr_plural``/``_cr_kind``), the
    ``_state_map`` from operator-specific lifecycle states to
    ``MaterializationJobStatus``, the resource name, and how to read the current
    state (and error message) out of the CR object.
    """

    # CR coordinates and state mapping — set by subclasses.
    _cr_group: str
    _cr_version: str
    _cr_plural: str
    _cr_kind: str
    _state_map: Dict[str, MaterializationJobStatus]

    def __init__(
        self,
        job_id: str,
        namespace: str,
        custom_api: client.CustomObjectsApi,
        error: Optional[BaseException] = None,
    ):
        super().__init__()
        self._job_id = job_id
        self.namespace = namespace
        self.custom_api = custom_api
        self._error: Optional[BaseException] = error
        # Once we observe SUCCEEDED/ERROR on the CR, keep it. The operator (and
        # its TTL) delete the CR after success; a later status() poll must not
        # turn that into a false "not found" failure.
        self._terminal_status: Optional[MaterializationJobStatus] = None
        if error is not None:
            self._terminal_status = MaterializationJobStatus.ERROR

    # ── Variant surface ─────────────────────────────────────────────
    @property
    def resource_name(self) -> str:
        """Kubernetes object name of this job's custom resource."""
        raise NotImplementedError

    def _extract_state(self, obj: dict) -> str:
        """Read the operator-specific lifecycle state string out of the CR."""
        raise NotImplementedError

    def _extract_error_message(self, obj: dict, state: str) -> str:
        """Human-readable error for a failed CR. Overridable for richer detail."""
        return f"{self._cr_kind} failed: {state}"

    # ── Shared machinery ────────────────────────────────────────────
    def status(self) -> MaterializationJobStatus:
        if self._terminal_status is not None:
            return self._terminal_status
        if self._error is not None:
            return MaterializationJobStatus.ERROR

        obj = self._get_cr_with_retry()
        if obj is None:
            return (
                MaterializationJobStatus.ERROR
                if self._error
                else MaterializationJobStatus.RUNNING
            )

        state = self._extract_state(obj)
        result = self._state_map.get(state, MaterializationJobStatus.WAITING)
        if result == MaterializationJobStatus.ERROR:
            self._error = Exception(self._extract_error_message(obj, state))
            self._terminal_status = MaterializationJobStatus.ERROR
        elif result == MaterializationJobStatus.SUCCEEDED:
            self._terminal_status = MaterializationJobStatus.SUCCEEDED
        return result

    def mark_succeeded(self) -> None:
        """Record successful completion before the CR is deleted/TTL'd."""
        self._error = None
        self._terminal_status = MaterializationJobStatus.SUCCEEDED

    def _get_cr_with_retry(self) -> Optional[dict]:
        """Fetch the custom resource with exponential backoff on transient errors."""
        last_exc = None
        for attempt in range(_MAX_RETRIES):
            try:
                return cast(
                    Optional[dict[Any, Any]],
                    self.custom_api.get_namespaced_custom_object(
                        group=self._cr_group,
                        version=self._cr_version,
                        namespace=self.namespace,
                        plural=self._cr_plural,
                        name=self.resource_name,
                    ),
                )
            except ApiException as e:
                if e.status == 404:
                    # Already completed jobs are cleaned up; don't invent a failure.
                    if self._terminal_status == MaterializationJobStatus.SUCCEEDED:
                        return None
                    self._error = Exception(
                        f"{self._cr_kind} {self.resource_name} not found"
                    )
                    return None
                if not _is_retryable(e):
                    self._error = Exception(
                        f"Kubernetes API error polling {self.resource_name}: "
                        f"HTTP {e.status} {e.reason}.{_rbac_hint(e.status)}"
                    )
                    return None
                last_exc = e
                wait = _RETRY_BACKOFF_BASE**attempt
                logger.warning(
                    f"API poll attempt {attempt + 1}/{_MAX_RETRIES} failed "
                    f"(HTTP {e.status}), retrying in {wait}s"
                )
                time.sleep(wait)
        self._error = Exception(
            f"Failed to poll {self._cr_kind} after {_MAX_RETRIES} attempts: "
            f"{last_exc.reason if last_exc else 'unknown'}"
        )
        return None

    def error(self) -> Optional[BaseException]:
        return self._error

    def should_be_retried(self) -> bool:
        return False

    def job_id(self) -> str:
        return self.resource_name

    def url(self) -> Optional[str]:
        return None
