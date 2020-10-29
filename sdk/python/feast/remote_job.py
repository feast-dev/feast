import time
from typing import Any, Callable, Dict, List

from feast.core.JobService_pb2 import CancelJobRequest, GetJobRequest, JobStatus
from feast.core.JobService_pb2_grpc import JobServiceStub
from feast.pyspark.abc import (
    BatchIngestionJob,
    RetrievalJob,
    SparkJobFailure,
    SparkJobStatus,
    StreamIngestionJob,
)

GrpcExtraParamProvider = Callable[[], Dict[str, Any]]


class RemoteJobMixin:
    def __init__(
        self,
        service: JobServiceStub,
        grpc_extra_param_provider: GrpcExtraParamProvider,
        job_id: str,
    ):
        """
        Args:
            service: Job service GRPC stub
            job_id: job reference
        """
        self._job_id = job_id
        self._service = service
        self._grpc_extra_param_provider = grpc_extra_param_provider

    def get_id(self) -> str:
        return self._job_id

    def get_status(self) -> SparkJobStatus:
        response = self._service.GetJob(
            GetJobRequest(job_id=self._job_id), **self._grpc_extra_param_provider()
        )

        if response.job.status == JobStatus.JOB_STATUS_RUNNING:
            return SparkJobStatus.IN_PROGRESS
        elif response.job.status == JobStatus.JOB_STATUS_PENDING:
            return SparkJobStatus.STARTING
        elif response.job.status == JobStatus.JOB_STATUS_DONE:
            return SparkJobStatus.COMPLETED
        elif response.job.status == JobStatus.JOB_STATUS_ERROR:
            return SparkJobStatus.FAILED
        else:
            # we should never get here
            raise Exception(f"Invalid remote job state {response.job.status}")

    def cancel(self):
        self._service.CancelJob(
            CancelJobRequest(job_id=self._job_id), **self._grpc_extra_param_provider()
        )

    def _wait_for_job_status(
        self, goal_status: List[SparkJobStatus], timeout_seconds=90
    ) -> SparkJobStatus:
        start_time = time.time()

        while time.time() < (start_time + timeout_seconds):
            status = self.get_status()
            if status in goal_status:
                return status
            else:
                time.sleep(1.0)
        else:
            raise TimeoutError("Timed out waiting for job status")


class RemoteRetrievalJob(RemoteJobMixin, RetrievalJob):
    """
    Historical feature retrieval job result, job being run remotely bt the job service
    """

    def __init__(
        self,
        service: JobServiceStub,
        grpc_extra_param_provider: GrpcExtraParamProvider,
        job_id: str,
        output_file_uri: str,
    ):
        """
        This is the job object representing the historical retrieval job.

        Args:
            output_file_uri (str): Uri to the historical feature retrieval job output file.
        """
        super().__init__(service, grpc_extra_param_provider, job_id)
        self._output_file_uri = output_file_uri

    def get_output_file_uri(self, timeout_sec=None):
        status = self._wait_for_job_status(
            goal_status=[SparkJobStatus.COMPLETED, SparkJobStatus.FAILED],
            timeout_seconds=600,
        )
        if status == SparkJobStatus.COMPLETED:
            return self._output_file_uri
        else:
            raise SparkJobFailure("Spark job failed")


class RemoteBatchIngestionJob(RemoteJobMixin, BatchIngestionJob):
    """
    Batch ingestion job result.
    """

    def __init__(
        self,
        service: JobServiceStub,
        grpc_extra_param_provider: GrpcExtraParamProvider,
        job_id: str,
    ):
        super().__init__(service, grpc_extra_param_provider, job_id)


class RemoteStreamIngestionJob(RemoteJobMixin, StreamIngestionJob):
    """
    Stream ingestion job result.
    """

    def __init__(
        self,
        service: JobServiceStub,
        grpc_extra_param_provider: GrpcExtraParamProvider,
        job_id: str,
    ):
        super().__init__(service, grpc_extra_param_provider, job_id)
