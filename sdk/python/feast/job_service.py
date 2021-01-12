import logging
import os
import signal
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple, cast

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import feast
from feast.constants import ConfigOptions as opt
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
    start_historical_feature_retrieval_job,
    start_offline_to_online_ingestion,
    start_stream_to_online_ingestion,
)
from feast.third_party.grpc.health.v1 import HealthService_pb2_grpc
from feast.third_party.grpc.health.v1.HealthService_pb2 import (
    HealthCheckResponse,
    ServingStatus,
)


def _job_to_proto(spark_job: SparkJob) -> JobProto:
    job = JobProto()
    job.id = spark_job.get_id()
    job.log_uri = cast(str, spark_job.get_log_uri() or "")
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
        job.batch_ingestion.table_name = spark_job.get_feature_table()
    elif isinstance(spark_job, StreamIngestionJob):
        job.type = JobType.STREAM_INGESTION_JOB
        job.stream_ingestion.table_name = spark_job.get_feature_table()
    else:
        raise ValueError(f"Invalid job type {job}")

    job.start_time.FromDatetime(spark_job.get_start_time())

    return job


class JobServiceServicer(JobService_pb2_grpc.JobServiceServicer):
    def __init__(self, client):
        self.client = client

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

        job_start_timestamp = Timestamp()
        job_start_timestamp.FromDatetime(job.get_start_time())

        return StartOfflineToOnlineIngestionJobResponse(
            id=job.get_id(),
            job_start_time=job_start_timestamp,
            table_name=request.table_name,
            log_uri=job.get_log_uri(),
        )

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

        job_start_timestamp = Timestamp()
        job_start_timestamp.FromDatetime(job.get_start_time())

        return GetHistoricalFeaturesResponse(
            id=job.get_id(),
            output_file_uri=output_file_uri,
            job_start_time=job_start_timestamp,
        )

    def StartStreamToOnlineIngestionJob(
        self, request: StartStreamToOnlineIngestionJobRequest, context
    ):
        """Start job to ingest data from stream into online store"""

        feature_table = self.client.get_feature_table(
            request.table_name, request.project
        )

        if self.client._config.getboolean(opt.JOB_SERVICE_ENABLE_CONTROL_LOOP):
            # If the control loop is enabled, return existing stream ingestion job id instead of starting a new one
            params = get_stream_to_online_ingestion_params(
                self.client, request.project, feature_table, []
            )
            job_hash = params.get_job_hash()
            for job in list_jobs(include_terminated=True, client=self.client):
                if isinstance(job, StreamIngestionJob) and job.get_hash() == job_hash:
                    job_start_timestamp = Timestamp()
                    job_start_timestamp.FromDatetime(job.get_start_time())
                    return StartStreamToOnlineIngestionJobResponse(
                        id=job.get_id(),
                        job_start_time=job_start_timestamp,
                        table_name=job.get_feature_table(),
                        log_uri=job.get_log_uri(),
                    )
            raise RuntimeError(
                "Feast Job Service has control loop enabled, but couldn't find the existing stream ingestion job for the given FeatureTable"
            )

        # TODO: add extra_jars to request
        job = start_stream_to_online_ingestion(
            client=self.client,
            project=request.project,
            feature_table=feature_table,
            extra_jars=[],
        )

        job_start_timestamp = Timestamp()
        job_start_timestamp.FromDatetime(job.get_start_time())
        return StartStreamToOnlineIngestionJobResponse(
            id=job.get_id(),
            job_start_time=job_start_timestamp,
            table_name=request.table_name,
            log_uri=job.get_log_uri(),
        )

    def ListJobs(self, request, context):
        """List all types of jobs"""
        jobs = list_jobs(
            include_terminated=request.include_terminated, client=self.client
        )
        return ListJobsResponse(jobs=[_job_to_proto(job) for job in jobs])

    def CancelJob(self, request, context):
        """Stop a single job"""
        job = get_job_by_id(request.job_id, client=self.client)
        job.cancel()
        return CancelJobResponse()

    def GetJob(self, request, context):
        """Get details of a single job"""
        job = get_job_by_id(request.job_id, client=self.client)
        return GetJobResponse(job=_job_to_proto(job))


def start_control_loop() -> None:
    """Starts control loop that continuously ensures that correct jobs are being run.

    Currently this affects only the stream ingestion jobs. Please refer to
    ensure_stream_ingestion_jobs for full documentation on how the check works.

    """
    logging.info(
        "Feast Job Service is starting a control loop in a background thread, "
        "which will ensure that stream ingestion jobs are successfully running."
    )
    try:
        client = feast.Client()
        while True:
            ensure_stream_ingestion_jobs(client, all_projects=True)
            time.sleep(1)
    except Exception:
        traceback.print_exc()
    finally:
        # Send interrupt signal to the main thread to kill the server if control loop fails
        os.kill(os.getpid(), signal.SIGINT)


class HealthServicer(HealthService_pb2_grpc.HealthServicer):
    def Check(self, request, context):
        return HealthCheckResponse(status=ServingStatus.SERVING)


class LoggingInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        logging.info(handler_call_details)
        return continuation(handler_call_details)


def start_job_service() -> None:
    """
    Start Feast Job Service
    """

    log_fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    client = feast.Client()

    if client._config.getboolean(opt.JOB_SERVICE_ENABLE_CONTROL_LOOP):
        # Start the control loop thread only if it's enabled from configs
        thread = threading.Thread(target=start_control_loop, daemon=True)
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


def _get_expected_job_hash_to_table_refs(
    client: feast.Client, projects: List[str]
) -> Dict[str, Tuple[str, str]]:
    """
    Checks all feature tables for the requires project(s) and determines all required stream
    ingestion jobs from them. Outputs a map of the expected job_hash to a tuple of (project, table_name).

    Args:
        all_projects (bool): If true, runs the check for all project.
            Otherwise only checks the current project.

    Returns:
        Dict[str, Tuple[str, str]]: Map of job_hash -> (project, table_name) for expected stream ingestion jobs
    """
    job_hash_to_table_refs = {}

    for project in projects:
        feature_tables = client.list_feature_tables(project)
        for feature_table in feature_tables:
            if feature_table.stream_source is not None:
                params = get_stream_to_online_ingestion_params(
                    client, project, feature_table, []
                )
                job_hash = params.get_job_hash()
                job_hash_to_table_refs[job_hash] = (project, feature_table.name)

    return job_hash_to_table_refs


def ensure_stream_ingestion_jobs(client: feast.Client, all_projects: bool):
    """Ensures all required stream ingestion jobs are running and cleans up the unnecessary jobs.

    More concretely, it will determine
    - which stream ingestion jobs are running
    - which stream ingestion jobs should be running
    And it'll do 2 kinds of operations
    - Cancel all running jobs that should not be running
    - Start all non-existent jobs that should be running

    Args:
        all_projects (bool): If true, runs the check for all project.
                             Otherwise only checks the client's current project.
    """

    projects = client.list_projects() if all_projects else [client.project]

    expected_job_hash_to_table_refs = _get_expected_job_hash_to_table_refs(
        client, projects
    )

    expected_job_hashes = set(expected_job_hash_to_table_refs.keys())

    jobs_by_hash: Dict[str, StreamIngestionJob] = {}
    for job in client.list_jobs(include_terminated=False):
        if isinstance(job, StreamIngestionJob):
            jobs_by_hash[job.get_hash()] = job

    existing_job_hashes = set(jobs_by_hash.keys())

    job_hashes_to_cancel = existing_job_hashes - expected_job_hashes
    job_hashes_to_start = expected_job_hashes - existing_job_hashes

    logging.debug(
        f"existing_job_hashes = {sorted(list(existing_job_hashes))} expected_job_hashes = {sorted(list(expected_job_hashes))}"
    )

    for job_hash in job_hashes_to_cancel:
        job = jobs_by_hash[job_hash]
        logging.info(
            f"Cancelling a stream ingestion job with job_hash={job_hash} job_id={job.get_id()} status={job.get_status()}"
        )
        job.cancel()

    for job_hash in job_hashes_to_start:
        # Any job that we wish to start should be among expected table refs map
        project, table_name = expected_job_hash_to_table_refs[job_hash]
        logging.info(
            f"Starting a stream ingestion job for project={project}, table_name={table_name} with job_hash={job_hash}"
        )
        feature_table = client.get_feature_table(name=table_name, project=project)
        client.start_stream_to_online_ingestion(feature_table, [], project=project)
