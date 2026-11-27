from typing import Optional

from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
)
from feast.infra.compute_engines.operator.job import (
    OperatorMaterializationJob,
)

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


class SparkApplicationMaterializationJob(OperatorMaterializationJob):
    """Tracks a ``SparkApplication`` custom resource (Kubeflow Spark Operator)."""

    _cr_group = "sparkoperator.k8s.io"
    _cr_version = "v1beta2"
    _cr_plural = "sparkapplications"
    _cr_kind = "SparkApplication"
    _state_map = _STATE_MAP

    @property
    def resource_name(self) -> str:
        return f"feast-sa-{self._job_id}"

    def _extract_state(self, obj: dict) -> str:
        return obj.get("status", {}).get("applicationState", {}).get("state", "")

    def _extract_error_message(self, obj: dict, state: str) -> str:
        return (
            obj.get("status", {})
            .get("applicationState", {})
            .get("errorMessage", f"SparkApplication failed: {state}")
        )
