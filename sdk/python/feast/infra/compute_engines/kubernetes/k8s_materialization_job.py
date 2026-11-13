from typing import Optional

from kubernetes import client

from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
)


class KubernetesMaterializationJob(MaterializationJob):
    def __init__(
        self,
        job_id: str,
        namespace: str,
        error: Optional[BaseException] = None,
    ):
        super().__init__()
        self._job_id = job_id
        self.namespace = namespace
        self._error: Optional[BaseException] = error
        self.batch_v1 = client.BatchV1Api()

    def error(self):
        return self._error

    def status(self):
        if self._error is not None:
            return MaterializationJobStatus.ERROR

        job_status = self.batch_v1.read_namespaced_job_status(
            self.job_id(), self.namespace
        ).status
        # Look at every condition, not just the first. Kubernetes 1.31+ adds
        # SuccessCriteriaMet or FailureTarget before Complete or Failed.
        for condition in job_status.conditions or []:
            if condition.status != "True":
                continue
            if condition.type == "Complete":
                return MaterializationJobStatus.SUCCEEDED
            if condition.type == "Failed":
                self._error = Exception(
                    f"Job {self.job_id()} failed with reason: {condition.message}"
                )
                return MaterializationJobStatus.ERROR

        if job_status.active:
            return MaterializationJobStatus.RUNNING
        return MaterializationJobStatus.WAITING

    def should_be_retried(self):
        return False

    def job_id(self):
        return f"feast-materialization-{self._job_id}"

    def url(self):
        return None
