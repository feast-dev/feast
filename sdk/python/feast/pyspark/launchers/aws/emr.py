import os
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional
from urllib.parse import urlunparse

import boto3
from botocore.config import Config as BotoConfig

from feast.pyspark.abc import (
    BatchIngestionJob,
    BatchIngestionJobParameters,
    JobLauncher,
    RetrievalJob,
    RetrievalJobParameters,
    SparkJob,
    SparkJobFailure,
    SparkJobStatus,
    StreamIngestionJob,
    StreamIngestionJobParameters,
)
from feast.staging.storage_client import get_staging_client

from .emr_utils import (
    FAILED_STEP_STATES,
    HISTORICAL_RETRIEVAL_JOB_TYPE,
    IN_PROGRESS_STEP_STATES,
    OFFLINE_TO_ONLINE_JOB_TYPE,
    STREAM_TO_ONLINE_JOB_TYPE,
    SUCCEEDED_STEP_STATES,
    TERMINAL_STEP_STATES,
    EmrJobRef,
    JobInfo,
    _cancel_job,
    _get_job_creation_time,
    _get_job_state,
    _historical_retrieval_step,
    _job_ref_to_str,
    _list_jobs,
    _load_new_cluster_template,
    _random_string,
    _stream_ingestion_step,
    _sync_offline_to_online_step,
    _upload_jar,
    _wait_for_job_state,
)


class EmrJobMixin:
    def __init__(self, emr_client, job_ref: EmrJobRef):
        """
        Args:
            emr_client: boto3 emr client
            job_ref: job reference
        """
        self._job_ref = job_ref
        self._emr_client = emr_client

    def get_id(self) -> str:
        return _job_ref_to_str(self._job_ref)

    def get_status(self) -> SparkJobStatus:
        emr_state = _get_job_state(self._emr_client, self._job_ref)
        if emr_state in IN_PROGRESS_STEP_STATES:
            return SparkJobStatus.IN_PROGRESS
        elif emr_state in SUCCEEDED_STEP_STATES:
            return SparkJobStatus.COMPLETED
        elif emr_state in FAILED_STEP_STATES:
            return SparkJobStatus.FAILED
        else:
            # we should never get here
            raise Exception("Invalid EMR state")

    def cancel(self):
        _cancel_job(self._emr_client, self._job_ref)

    def get_start_time(self) -> datetime:
        return _get_job_creation_time(self._emr_client, self._job_ref)


class EmrRetrievalJob(EmrJobMixin, RetrievalJob):
    """
    Historical feature retrieval job result for a EMR cluster
    """

    def __init__(self, emr_client, job_ref: EmrJobRef, output_file_uri: str):
        """
        This is the job object representing the historical retrieval job, returned by EmrClusterLauncher.

        Args:
            output_file_uri (str): Uri to the historical feature retrieval job output file.
        """
        super().__init__(emr_client, job_ref)
        self._output_file_uri = output_file_uri

    def get_output_file_uri(self, timeout_sec=None, block=True):
        if not block:
            return self._output_file_uri

        state = _wait_for_job_state(
            self._emr_client, self._job_ref, TERMINAL_STEP_STATES, timeout_sec
        )
        if state in SUCCEEDED_STEP_STATES:
            return self._output_file_uri
        else:
            raise SparkJobFailure("Spark job failed")


class EmrBatchIngestionJob(EmrJobMixin, BatchIngestionJob):
    """
    Ingestion job result for a EMR cluster
    """

    def __init__(self, emr_client, job_ref: EmrJobRef, table_name: str):
        super().__init__(emr_client, job_ref)
        self._table_name = table_name

    def get_feature_table(self) -> str:
        return self._table_name


class EmrStreamIngestionJob(EmrJobMixin, StreamIngestionJob):
    """
    Ingestion streaming job for a EMR cluster
    """

    def __init__(self, emr_client, job_ref: EmrJobRef, job_hash: str, table_name: str):
        super().__init__(emr_client, job_ref)
        self._job_hash = job_hash
        self._table_name = table_name

    def get_hash(self) -> str:
        return self._job_hash

    def get_feature_table(self) -> str:
        return self._table_name


class EmrClusterLauncher(JobLauncher):
    """
    Submits jobs to an existing or new EMR cluster. Requires boto3 as an additional dependency.
    """

    _existing_cluster_id: Optional[str]
    _new_cluster_template: Optional[Dict[str, Any]]
    _staging_location: str
    _emr_log_location: str
    _region: str

    def __init__(
        self,
        *,
        region: str,
        existing_cluster_id: Optional[str],
        new_cluster_template_path: Optional[str],
        staging_location: str,
        emr_log_location: str,
    ):
        """
        Initialize a dataproc job controller client, used internally for job submission and result
        retrieval. Can work with either an existing EMR cluster, or create a cluster on-demand
        for each job.

        Args:
            region (str):
                AWS region name.
            existing_cluster_id (str):
                Existing EMR cluster id, if using an existing cluster.
            new_cluster_template_path (str):
                Path to yaml new cluster template, if using a new cluster.
            staging_location:
                An S3 staging location for artifacts.
            emr_log_location:
                S3 location for EMR logs.
        """

        assert existing_cluster_id or new_cluster_template_path

        self._existing_cluster_id = existing_cluster_id
        if new_cluster_template_path:
            self._new_cluster_template = _load_new_cluster_template(
                new_cluster_template_path
            )
        else:
            self._new_cluster_template = None

        self._staging_location = staging_location
        self._emr_log_location = emr_log_location
        self._region = region

    def _emr_client(self):

        # Use an increased number of retries since DescribeStep calls have a pretty low rate limit.
        config = BotoConfig(retries={"max_attempts": 10, "mode": "standard"})
        return boto3.client("emr", region_name=self._region, config=config)

    def _submit_emr_job(self, step: Dict[str, Any]) -> EmrJobRef:
        """
        Submit EMR job using a new or existing cluster.

        Returns a job reference (cluster_id and step_id).
        """

        emr = self._emr_client()

        if self._existing_cluster_id:
            step["ActionOnFailure"] = "CONTINUE"
            step_ids = emr.add_job_flow_steps(
                JobFlowId=self._existing_cluster_id, Steps=[step],
            )
            return EmrJobRef(self._existing_cluster_id, step_ids["StepIds"][0])
        else:
            assert self._new_cluster_template is not None
            jobTemplate = self._new_cluster_template
            step["ActionOnFailure"] = "TERMINATE_CLUSTER"

            jobTemplate["Steps"] = [step]

            if self._emr_log_location:
                jobTemplate["LogUri"] = os.path.join(
                    self._emr_log_location, _random_string(5)
                )

            job = emr.run_job_flow(**jobTemplate)
            return EmrJobRef(job["JobFlowId"], None)

    def historical_feature_retrieval(
        self, job_params: RetrievalJobParameters
    ) -> RetrievalJob:

        with open(job_params.get_main_file_path()) as f:
            pyspark_script = f.read()

        pyspark_script_path = urlunparse(
            get_staging_client("s3").upload_fileobj(
                BytesIO(pyspark_script.encode("utf8")),
                local_path="historical_retrieval.py",
                remote_path_prefix=self._staging_location,
                remote_path_suffix=".py",
            )
        )

        step = _historical_retrieval_step(
            pyspark_script_path,
            args=job_params.get_arguments(),
            output_file_uri=job_params.get_destination_path(),
            packages=job_params.get_extra_packages(),
        )

        job_ref = self._submit_emr_job(step)

        return EmrRetrievalJob(
            self._emr_client(), job_ref, job_params.get_destination_path(),
        )

    def offline_to_online_ingestion(
        self, ingestion_job_params: BatchIngestionJobParameters
    ) -> BatchIngestionJob:
        """
        Submits a batch ingestion job to a Spark cluster.

        Raises:
            SparkJobFailure: The spark job submission failed, encountered error
                during execution, or timeout.

        Returns:
            BatchIngestionJob: wrapper around remote job that can be used to check when job completed.
        """

        jar_s3_path = _upload_jar(
            self._staging_location, ingestion_job_params.get_main_file_path()
        )
        step = _sync_offline_to_online_step(
            jar_s3_path,
            ingestion_job_params.get_feature_table_name(),
            args=ingestion_job_params.get_arguments(),
        )

        job_ref = self._submit_emr_job(step)

        return EmrBatchIngestionJob(
            self._emr_client(), job_ref, ingestion_job_params.get_feature_table_name()
        )

    def start_stream_to_online_ingestion(
        self, ingestion_job_params: StreamIngestionJobParameters
    ) -> StreamIngestionJob:
        """
        Starts a stream ingestion job on a Spark cluster.

        Returns:
            StreamIngestionJob: wrapper around remote job that can be used to check on the job.
        """
        jar_s3_path = _upload_jar(
            self._staging_location, ingestion_job_params.get_main_file_path()
        )

        extra_jar_paths: List[str] = []
        for extra_jar in ingestion_job_params.get_extra_jar_paths():
            if extra_jar.startswith("s3://"):
                extra_jar_paths.append(extra_jar)
            else:
                extra_jar_paths.append(_upload_jar(self._staging_location, extra_jar))

        job_hash = ingestion_job_params.get_job_hash()

        step = _stream_ingestion_step(
            jar_s3_path,
            extra_jar_paths,
            ingestion_job_params.get_feature_table_name(),
            args=ingestion_job_params.get_arguments(),
            job_hash=job_hash,
        )

        job_ref = self._submit_emr_job(step)

        return EmrStreamIngestionJob(
            self._emr_client(),
            job_ref,
            job_hash,
            ingestion_job_params.get_feature_table_name(),
        )

    def _job_from_job_info(self, job_info: JobInfo) -> SparkJob:
        if job_info.job_type == HISTORICAL_RETRIEVAL_JOB_TYPE:
            assert job_info.output_file_uri is not None
            return EmrRetrievalJob(
                emr_client=self._emr_client(),
                job_ref=job_info.job_ref,
                output_file_uri=job_info.output_file_uri,
            )
        elif job_info.job_type == OFFLINE_TO_ONLINE_JOB_TYPE:
            table_name = job_info.table_name if job_info.table_name else ""
            assert table_name is not None
            return EmrBatchIngestionJob(
                emr_client=self._emr_client(),
                job_ref=job_info.job_ref,
                table_name=table_name,
            )
        elif job_info.job_type == STREAM_TO_ONLINE_JOB_TYPE:
            table_name = job_info.table_name if job_info.table_name else ""
            assert table_name is not None
            # job_hash must not be None for stream ingestion jobs
            assert job_info.job_hash is not None
            return EmrStreamIngestionJob(
                emr_client=self._emr_client(),
                job_ref=job_info.job_ref,
                job_hash=job_info.job_hash,
                table_name=table_name,
            )
        else:
            # We should never get here
            raise ValueError(f"Unknown job type {job_info.job_type}")

    def list_jobs(self, include_terminated: bool) -> List[SparkJob]:
        """
        Find EMR job by a string id.

        Args:
            include_terminated: whether to include terminated jobs.

        Returns:
            A list of SparkJob instances.
        """

        jobs = _list_jobs(
            emr_client=self._emr_client(),
            job_type=None,
            table_name=None,
            active_only=not include_terminated,
        )

        result = []
        for job_info in jobs:
            result.append(self._job_from_job_info(job_info))
        return result

    def get_job_by_id(self, job_id: str) -> SparkJob:
        """
        Find EMR job by a string id. Note that it will also return terminated jobs.

        Raises:
            KeyError if the job not found.
        """
        # FIXME: this doesn't have to be a linear search but that'll do for now
        jobs = _list_jobs(
            emr_client=self._emr_client(),
            job_type=None,
            table_name=None,
            active_only=False,
        )

        for job_info in jobs:
            if _job_ref_to_str(job_info.job_ref) == job_id:
                return self._job_from_job_info(job_info)
        else:
            raise KeyError(f"Job not found {job_id}")
