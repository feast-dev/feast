import logging
import time
from typing import Optional

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
            " Role/RoleBinding for sparkapplications and configmaps in this namespace."
        )
    return ""


_STATE_MAP = {
    "": MaterializationJobStatus.WAITING,
    "SUBMITTED": MaterializationJobStatus.WAITING,
    "RUNNING": MaterializationJobStatus.RUNNING,
    "COMPLETED": MaterializationJobStatus.SUCCEEDED,
    "FAILED": MaterializationJobStatus.ERROR,
    "SUBMISSION_FAILED": MaterializationJobStatus.ERROR,
    "PENDING_RERUN": MaterializationJobStatus.WAITING,
    "INVALIDATING": MaterializationJobStatus.WAITING,
    "SUCCEEDING": MaterializationJobStatus.RUNNING,
    "FAILING": MaterializationJobStatus.RUNNING,
    "SUSPENDING": MaterializationJobStatus.WAITING,
    "SUSPENDED": MaterializationJobStatus.WAITING,
    "RESUMING": MaterializationJobStatus.WAITING,
    "UNKNOWN": MaterializationJobStatus.WAITING,
}
assert len(_STATE_MAP) == 14


class CompletedMaterializationJob(MaterializationJob):
    """Lightweight stub for a FV whose materialization already succeeded.

    Used by ``_build_per_fv_jobs`` so that each FV gets an independent job
    object.  Unlike ``SparkApplicationMaterializationJob``, this never polls
    the K8s API — the outcome is already known from the registry state.
    """

    def __init__(self, job_id: str):
        super().__init__()
        self._job_id = job_id

    def status(self) -> MaterializationJobStatus:
        return MaterializationJobStatus.SUCCEEDED

    def error(self) -> Optional[BaseException]:
        return None

    def should_be_retried(self) -> bool:
        return False

    def job_id(self) -> str:
        return f"feast-sa-{self._job_id}"

    def url(self) -> Optional[str]:
        return None


class SparkApplicationMaterializationJob(MaterializationJob):
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

    def status(self) -> MaterializationJobStatus:
        if self._error is not None:
            return MaterializationJobStatus.ERROR

        obj = self._get_cr_with_retry()
        if obj is None:
            return (
                MaterializationJobStatus.ERROR
                if self._error
                else MaterializationJobStatus.RUNNING
            )

        state = obj.get("status", {}).get("applicationState", {}).get("state", "")
        result = _STATE_MAP.get(state, MaterializationJobStatus.WAITING)
        if result == MaterializationJobStatus.ERROR:
            msg = (
                obj.get("status", {})
                .get("applicationState", {})
                .get("errorMessage", f"SparkApplication failed: {state}")
            )
            self._error = Exception(msg)
        return result

    def _get_cr_with_retry(self) -> Optional[dict]:
        """Fetch SparkApplication CR with exponential backoff on transient errors."""
        last_exc = None
        for attempt in range(_MAX_RETRIES):
            try:
                return self.custom_api.get_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=self.namespace,
                    plural="sparkapplications",
                    name=f"feast-sa-{self._job_id}",
                )
            except ApiException as e:
                if e.status == 404:
                    self._error = Exception(
                        f"SparkApplication feast-sa-{self._job_id} not found"
                    )
                    return None
                if not _is_retryable(e):
                    self._error = Exception(
                        f"Kubernetes API error polling feast-sa-{self._job_id}: "
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
            f"Failed to poll SparkApplication after {_MAX_RETRIES} attempts: "
            f"{last_exc.reason if last_exc else 'unknown'}"
        )
        return None

    def error(self) -> Optional[BaseException]:
        return self._error

    def should_be_retried(self) -> bool:
        return False

    def job_id(self) -> str:
        return f"feast-sa-{self._job_id}"

    def url(self) -> Optional[str]:
        return None
