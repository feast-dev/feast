from typing import Optional

from kubernetes import client

from feast.infra.materialization.batch_materialization_engine import (
    MaterializationJob,
    MaterializationJobStatus,
)


class BytewaxMaterializationJob(MaterializationJob):
    def __init__(
        self,
        job_id,
        namespace,
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
        else:
            # TODO: Find a better way to parse status?
            job_status = self.batch_v1.read_namespaced_job_status(
                self.job_id(), self.namespace
            ).status
            if job_status.active is not None:
                if job_status.completion_time is None:
                    return MaterializationJobStatus.RUNNING
            elif job_status.failed is not None:
                return MaterializationJobStatus.ERROR
            elif job_status.active is None and job_status.succeeded is not None:
                if job_status.conditions[0].type == "Complete":
                    return MaterializationJobStatus.SUCCEEDED

    def should_be_retried(self):
        return False

    def job_id(self):
        return f"dataflow-{self._job_id}"

    def url(self):
        return None
