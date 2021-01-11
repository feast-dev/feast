import time
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from feast.core.JobService_pb2 import CancelJobRequest, GetJobRequest
from feast.core.JobService_pb2 import Job as JobProto
from feast.core.JobService_pb2 import JobStatus, JobType
from feast.core.JobService_pb2_grpc import JobServiceStub
from feast.pyspark.abc import (
    BatchIngestionJob,
    RetrievalJob,
    SparkJob,
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
        start_time: datetime,
        log_uri: Optional[str],
    ):
        """
        Args:
            service: Job service GRPC stub
            job_id: job reference
        """
        self._job_id = job_id
        self._service = service
        self._grpc_extra_param_provider = grpc_extra_param_provider
        self._start_time = start_time
        self._log_uri = log_uri

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

    def get_start_time(self) -> datetime:
        return self._start_time

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

    def get_log_uri(self) -> Optional[str]:
        return self._log_uri


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
        start_time: datetime,
        log_uri: Optional[str],
    ):
        """
        This is the job object representing the historical retrieval job.

        Args:
            output_file_uri (str): Uri to the historical feature retrieval job output file.
        """
        super().__init__(
            service, grpc_extra_param_provider, job_id, start_time, log_uri
        )
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
        feature_table: str,
        start_time: datetime,
        log_uri: Optional[str],
    ):
        super().__init__(
            service, grpc_extra_param_provider, job_id, start_time, log_uri
        )
        self._feature_table = feature_table

    def get_feature_table(self) -> str:
        return self._feature_table


class RemoteStreamIngestionJob(RemoteJobMixin, StreamIngestionJob):
    """
    Stream ingestion job result.
    """

    def __init__(
        self,
        service: JobServiceStub,
        grpc_extra_param_provider: GrpcExtraParamProvider,
        job_id: str,
        feature_table: str,
        start_time: datetime,
        log_uri: Optional[str],
    ):
        super().__init__(
            service, grpc_extra_param_provider, job_id, start_time, log_uri
        )
        self._feature_table = feature_table

    def get_hash(self) -> str:
        response = self._service.GetJob(
            GetJobRequest(job_id=self._job_id), **self._grpc_extra_param_provider()
        )

        return response.job.hash

    def get_feature_table(self) -> str:
        return self._feature_table


def get_remote_job_from_proto(
    service: JobServiceStub,
    grpc_extra_param_provider: GrpcExtraParamProvider,
    job: JobProto,
) -> SparkJob:
    """Get the remote job python object from Job proto.

    Args:
        service (JobServiceStub): Reference to Job Service
        grpc_extra_param_provider (GrpcExtraParamProvider): Callable for providing extra parameters to grpc requests
        job (JobProto): Proto object describing the Job

    Returns:
        (SparkJob): A remote job object for the given job
    """

    if job.type == JobType.RETRIEVAL_JOB:
        return RemoteRetrievalJob(
            service,
            grpc_extra_param_provider,
            job.id,
            job.retrieval.output_location,
            job.start_time.ToDatetime(),
            job.log_uri,
        )
    elif job.type == JobType.BATCH_INGESTION_JOB:
        return RemoteBatchIngestionJob(
            service,
            grpc_extra_param_provider,
            job.id,
            job.batch_ingestion.table_name,
            job.start_time.ToDatetime(),
            job.log_uri,
        )
    elif job.type == JobType.STREAM_INGESTION_JOB:
        return RemoteStreamIngestionJob(
            service,
            grpc_extra_param_provider,
            job.id,
            job.stream_ingestion.table_name,
            job.start_time.ToDatetime(),
            job.log_uri,
        )
    else:
        raise ValueError(
            f"Invalid Job Type {job.type}, has to be one of "
            f"{(JobType.RETRIEVAL_JOB, JobType.BATCH_INGESTION_JOB, JobType.STREAM_INGESTION_JOB)}"
        )
