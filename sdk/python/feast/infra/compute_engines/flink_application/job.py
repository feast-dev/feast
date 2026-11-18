from typing import Optional

from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
)
from feast.infra.compute_engines.operator.job import OperatorMaterializationJob

# Flink's org.apache.flink.api.common.JobStatus values, mapped to Feast's
# MaterializationJobStatus. CANCELED is treated as a failure for a batch
# materialization (the job did not run to completion).
_STATE_MAP = {
    "": MaterializationJobStatus.WAITING,
    "INITIALIZING": MaterializationJobStatus.WAITING,
    "CREATED": MaterializationJobStatus.WAITING,
    "RECONCILING": MaterializationJobStatus.WAITING,
    "RESTARTING": MaterializationJobStatus.WAITING,
    "SUSPENDED": MaterializationJobStatus.WAITING,
    "RUNNING": MaterializationJobStatus.RUNNING,
    "FAILING": MaterializationJobStatus.RUNNING,
    "CANCELLING": MaterializationJobStatus.RUNNING,
    "FINISHED": MaterializationJobStatus.SUCCEEDED,
    "FAILED": MaterializationJobStatus.ERROR,
    "CANCELED": MaterializationJobStatus.ERROR,
}
assert len(_STATE_MAP) == 12


class CompletedMaterializationJob(MaterializationJob):
    """Stub for a FV whose materialization already succeeded.

    Used by ``_build_per_fv_jobs`` so each FV gets an independent job object.
    Unlike ``FlinkDeploymentMaterializationJob``, this never polls the K8s API —
    the outcome is already known from registry state.
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
        return f"feast-flink-{self._job_id}"

    def url(self) -> Optional[str]:
        return None


class FlinkDeploymentMaterializationJob(OperatorMaterializationJob):
    """Tracks a ``FlinkDeployment`` custom resource (Apache Flink K8s Operator)."""

    _cr_group = "flink.apache.org"
    _cr_version = "v1beta1"
    _cr_plural = "flinkdeployments"
    _cr_kind = "FlinkDeployment"
    _state_map = _STATE_MAP

    @property
    def resource_name(self) -> str:
        return f"feast-flink-{self._job_id}"

    def _extract_state(self, obj: dict) -> str:
        status = obj.get("status", {})
        job_state = status.get("jobStatus", {}).get("state", "")
        if job_state:
            return job_state
        # Before the job starts (or if it never starts), fall back to the
        # deployment lifecycleState so a pre-job failure is still terminal.
        if status.get("lifecycleState") == "FAILED":
            return "FAILED"
        return ""

    def _extract_error_message(self, obj: dict, state: str) -> str:
        return obj.get("status", {}).get("error") or f"FlinkDeployment failed: {state}"
