import os
import tempfile
from io import BytesIO
from typing import Any, Dict, Optional

import boto3
import pandas

from feast.data_source import FileSource
from feast.pyspark.abc import (
    IngestionJob,
    IngestionJobParameters,
    JobLauncher,
    RetrievalJob,
    RetrievalJobParameters,
    SparkJobStatus,
)

from .emr_utils import (
    FAILED_STEP_STATES,
    IN_PROGRESS_STEP_STATES,
    SUCCEEDED_STEP_STATES,
    TERMINAL_STEP_STATES,
    EmrJobRef,
    _get_job_state,
    _historical_retrieval_step,
    _load_new_cluster_template,
    _random_string,
    _s3_upload,
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
        return f'{self._job_ref.cluster_id}:{self._job_ref.step_id or ""}'

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

    def get_output_file_uri(self, timeout_sec=None):
        _wait_for_job_state(
            self._emr_client, self._job_ref, TERMINAL_STEP_STATES, timeout_sec
        )
        return self._output_file_uri


class EmrIngestionJob(EmrJobMixin, IngestionJob):
    """
    Ingestion job result for a EMR cluster
    """

    def __init__(self, emr_client, job_ref: EmrJobRef):
        super().__init__(emr_client, job_ref)


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
        return boto3.client("emr", region_name=self._region)

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

        pyspark_script_path = _s3_upload(
            BytesIO(pyspark_script.encode("utf8")),
            local_path="historical_retrieval.py",
            remote_path_prefix=self._staging_location,
            remote_path_suffix=".py",
        )

        step = _historical_retrieval_step(
            pyspark_script_path, args=job_params.get_arguments()
        )

        job_ref = self._submit_emr_job(step)

        return EmrRetrievalJob(
            self._emr_client(),
            job_ref,
            os.path.join(job_params.get_destination_path(), _random_string(8)),
        )

    def offline_to_online_ingestion(
        self, ingestion_job_params: IngestionJobParameters
    ) -> IngestionJob:
        """
        Submits a batch ingestion job to a Spark cluster.

        Raises:
            SparkJobFailure: The spark job submission failed, encountered error
                during execution, or timeout.

        Returns:
            IngestionJob: wrapper around remote job that can be used to check when job completed.
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

        return EmrIngestionJob(self._emr_client(), job_ref)

    def stage_dataframe(
        self, df: pandas.DataFrame, event_timestamp: str, created_timestamp_column: str
    ) -> FileSource:
        with tempfile.NamedTemporaryFile() as f:
            df.to_parquet(f)
            file_url = _s3_upload(
                f,
                f.name,
                remote_path_prefix=os.path.join(self._staging_location, "dataframes"),
                remote_path_suffix=".parquet",
            )
        return FileSource(
            event_timestamp_column=event_timestamp,
            created_timestamp_column=created_timestamp_column,
            file_format="parquet",
            file_url=file_url,
        )
