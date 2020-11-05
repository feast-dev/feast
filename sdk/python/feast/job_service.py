import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Set, Tuple

import grpc

import feast
from feast.constants import CONFIG_JOB_SERVICE_ENABLE_CONTROL_LOOP
from feast.core import JobService_pb2_grpc
from feast.core.JobService_pb2 import (
    CancelJobResponse,
    GetHistoricalFeaturesRequest,
    GetHistoricalFeaturesResponse,
    GetJobResponse,
)
from feast.core.JobService_pb2 import Job as JobProto
from feast.core.JobService_pb2 import (
    JobStatus,
    JobType,
    ListJobsResponse,
    StartOfflineToOnlineIngestionJobRequest,
    StartOfflineToOnlineIngestionJobResponse,
    StartStreamToOnlineIngestionJobRequest,
    StartStreamToOnlineIngestionJobResponse,
)
from feast.data_source import DataSource
from feast.pyspark.abc import (
    BatchIngestionJob,
    RetrievalJob,
    SparkJob,
    SparkJobStatus,
    StreamIngestionJob,
)
from feast.pyspark.launcher import (
    get_job_by_id,
    get_stream_to_online_ingestion_params,
    list_jobs,
    list_jobs_by_hash,
    start_historical_feature_retrieval_job,
    start_offline_to_online_ingestion,
    start_stream_to_online_ingestion,
)
from feast.third_party.grpc.health.v1 import HealthService_pb2_grpc
from feast.third_party.grpc.health.v1.HealthService_pb2 import (
    HealthCheckResponse,
    ServingStatus,
)


class JobServiceServicer(JobService_pb2_grpc.JobServiceServicer):
    def __init__(self, client):
        self.client = client

    def _job_to_proto(self, spark_job: SparkJob) -> JobProto:
        job = JobProto()
        job.id = spark_job.get_id()
        status = spark_job.get_status()
        if status == SparkJobStatus.COMPLETED:
            job.status = JobStatus.JOB_STATUS_DONE
        elif status == SparkJobStatus.IN_PROGRESS:
            job.status = JobStatus.JOB_STATUS_RUNNING
        elif status == SparkJobStatus.FAILED:
            job.status = JobStatus.JOB_STATUS_ERROR
        elif status == SparkJobStatus.STARTING:
            job.status = JobStatus.JOB_STATUS_PENDING
        else:
            raise ValueError(f"Invalid job status {status}")

        if isinstance(spark_job, RetrievalJob):
            job.type = JobType.RETRIEVAL_JOB
            job.retrieval.output_location = spark_job.get_output_file_uri(block=False)
        elif isinstance(spark_job, BatchIngestionJob):
            job.type = JobType.BATCH_INGESTION_JOB
        elif isinstance(spark_job, StreamIngestionJob):
            job.type = JobType.STREAM_INGESTION_JOB
        else:
            raise ValueError(f"Invalid job type {job}")

        return job

    def StartOfflineToOnlineIngestionJob(
        self, request: StartOfflineToOnlineIngestionJobRequest, context
    ):
        """Start job to ingest data from offline store into online store"""
        feature_table = self.client.get_feature_table(
            request.table_name, request.project
        )
        job = start_offline_to_online_ingestion(
            client=self.client,
            project=request.project,
            feature_table=feature_table,
            start=request.start_date.ToDatetime(),
            end=request.end_date.ToDatetime(),
        )
        return StartOfflineToOnlineIngestionJobResponse(id=job.get_id())

    def GetHistoricalFeatures(self, request: GetHistoricalFeaturesRequest, context):
        """Produce a training dataset, return a job id that will provide a file reference"""
        job = start_historical_feature_retrieval_job(
            client=self.client,
            project=request.project,
            entity_source=DataSource.from_proto(request.entity_source),
            feature_tables=self.client._get_feature_tables_from_feature_refs(
                list(request.feature_refs), request.project
            ),
            output_format=request.output_format,
            output_path=request.output_location,
        )

        output_file_uri = job.get_output_file_uri(block=False)

        return GetHistoricalFeaturesResponse(
            id=job.get_id(), output_file_uri=output_file_uri
        )

    def StartStreamToOnlineIngestionJob(
        self, request: StartStreamToOnlineIngestionJobRequest, context
    ):
        """Start job to ingest data from stream into online store"""

        feature_table = self.client.get_feature_table(
            request.table_name, request.project
        )

        if self.client._config.get(CONFIG_JOB_SERVICE_ENABLE_CONTROL_LOOP) != "False":
            # If the control loop is enabled, return existing stream ingestion job id instead of starting a new one
            params = get_stream_to_online_ingestion_params(
                self.client, request.project, feature_table, []
            )
            job_hash = params.get_job_hash()
            jobs_by_hash = list_jobs_by_hash(self.client, include_terminated=False)
            if job_hash not in jobs_by_hash:
                raise RuntimeError(
                    "Feast Job Service has control loop enabled, but couldn't find the existing stream ingestion job for the given FeatureTable"
                )
            return StartStreamToOnlineIngestionJobResponse(
                id=jobs_by_hash[job_hash].get_id()
            )

        # TODO: add extra_jars to request
        job = start_stream_to_online_ingestion(
            client=self.client,
            project=request.project,
            feature_table=feature_table,
            extra_jars=[],
        )
        return StartStreamToOnlineIngestionJobResponse(id=job.get_id())

    def ListJobs(self, request, context):
        """List all types of jobs"""
        jobs = list_jobs(
            include_terminated=request.include_terminated, client=self.client
        )
        return ListJobsResponse(jobs=[self._job_to_proto(job) for job in jobs])

    def CancelJob(self, request, context):
        """Stop a single job"""
        job = get_job_by_id(request.job_id, client=self.client)
        job.cancel()
        return CancelJobResponse()

    def GetJob(self, request, context):
        """Get details of a single job"""
        job = get_job_by_id(request.job_id, client=self.client)
        return GetJobResponse(job=self._job_to_proto(job))


def run_control_loop():
    """Runs control loop that continuously ensures that correct jobs are being run."""
    logging.info(
        "Feast Job Service is starting a control loop in a background thread..."
    )
    client = feast.Client()
    while True:
        # Map of job hash -> (project, table_name)
        table_metadata: Dict[str, Tuple[str, str]] = dict()
        # Job hashes that should exist
        final_job_hashes: Set[str] = set()

        for project in client.list_projects():
            feature_tables = client.list_feature_tables(project)
            for feature_table in feature_tables:
                if feature_table.stream_source is not None:
                    params = get_stream_to_online_ingestion_params(
                        client, project, feature_table, []
                    )
                    job_hash = params.get_job_hash()
                    final_job_hashes.add(job_hash)
                    table_metadata[job_hash] = (project, feature_table.name)

        jobs_by_hash = list_jobs_by_hash(client, include_terminated=False)
        # Job hashes that currently exist
        existing_job_hashes = set(jobs_by_hash.keys())

        job_hashes_to_cancel = existing_job_hashes - final_job_hashes
        job_hashes_to_start = final_job_hashes - existing_job_hashes

        for job_hash in job_hashes_to_cancel:
            job = jobs_by_hash[job_hash]
            logging.info(
                f"Cancelling a stream ingestion job with job_hash={job_hash} job_id={job.get_id()} status={job.get_status()}"
            )
            job.cancel()

        for job_hash in job_hashes_to_start:
            project, table_name = table_metadata[job_hash]
            logging.info(
                f"Starting a stream ingestion job for project={project}, table_name={table_name} with job_hash={job_hash}"
            )
            feature_table = client.get_feature_table(name=table_name, project=project)
            start_stream_to_online_ingestion(client, project, feature_table, [])

        time.sleep(1)


class HealthServicer(HealthService_pb2_grpc.HealthServicer):
    def Check(self, request, context):
        return HealthCheckResponse(status=ServingStatus.SERVING)


class LoggingInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        logging.info(handler_call_details)
        return continuation(handler_call_details)


def start_job_service():
    """
    Start Feast Job Service
    """

    log_fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    client = feast.Client()

    if client._config.get(CONFIG_JOB_SERVICE_ENABLE_CONTROL_LOOP) != "False":
        # Start the control loop thread only if it's enabled from configs
        thread = threading.Thread(target=run_control_loop, daemon=True)
        thread.start()

    server = grpc.server(ThreadPoolExecutor(), interceptors=(LoggingInterceptor(),))
    JobService_pb2_grpc.add_JobServiceServicer_to_server(
        JobServiceServicer(client), server
    )
    HealthService_pb2_grpc.add_HealthServicer_to_server(HealthServicer(), server)
    server.add_insecure_port("[::]:6568")
    server.start()
    logging.info("Feast Job Service is listening on port :6568")
    server.wait_for_termination()
