import os
import time
import uuid
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from google.cloud.dataproc_v1 import Job, JobControllerClient, JobStatus

from feast.pyspark.abc import (
    BatchIngestionJob,
    BatchIngestionJobParameters,
    JobLauncher,
    RetrievalJob,
    RetrievalJobParameters,
    SparkJob,
    SparkJobFailure,
    SparkJobParameters,
    SparkJobStatus,
    SparkJobType,
    StreamIngestionJob,
    StreamIngestionJobParameters,
)
from feast.staging.storage_client import get_staging_client


class DataprocJobMixin:
    def __init__(
        self,
        job: Job,
        refresh_fn: Callable[[], Job],
        cancel_fn: Callable[[], None],
        project: str,
        region: str,
    ):
        """
        Implementation of common methods for different types of SparkJob running on Dataproc cluster.

        Args:
            job (Job): Dataproc job resource.
            refresh_fn (Callable[[], Job]): A function that returns the latest job resource.
            cancel_fn (Callable[[], None]): A function that cancel the current job.
        """
        self._job = job
        self._refresh_fn = refresh_fn
        self._cancel_fn = cancel_fn
        self._project = project
        self._region = region

    def get_id(self) -> str:
        """
        Getter for the job id.

        Returns:
            str: Dataproc job id.
        """
        return self._job.reference.job_id

    def get_status(self) -> SparkJobStatus:
        """
        Job Status retrieval

        Returns:
            SparkJobStatus: Job status
        """
        self._job = self._refresh_fn()
        status = self._job.status
        if status.state in (
            JobStatus.State.ERROR,
            JobStatus.State.CANCEL_PENDING,
            JobStatus.State.CANCEL_STARTED,
            JobStatus.State.CANCELLED,
        ):
            return SparkJobStatus.FAILED
        elif status.state == JobStatus.State.RUNNING:
            return SparkJobStatus.IN_PROGRESS
        elif status.state in (
            JobStatus.State.PENDING,
            JobStatus.State.SETUP_DONE,
            JobStatus.State.STATE_UNSPECIFIED,
        ):
            return SparkJobStatus.STARTING

        return SparkJobStatus.COMPLETED

    def cancel(self):
        """
        Manually terminate job
        """
        self._cancel_fn()

    def get_error_message(self) -> Optional[str]:
        """
        Getter for the job's error message if applicable.

        Returns:
            str: Status detail of the job. Return None if the job is successful.
        """
        self._job = self._refresh_fn()
        status = self._job.status
        if status.state == JobStatus.State.ERROR:
            return status.details
        elif status.state in (
            JobStatus.State.CANCEL_PENDING,
            JobStatus.State.CANCEL_STARTED,
            JobStatus.State.CANCELLED,
        ):
            return "Job was cancelled."
        return None

    def block_polling(self, interval_sec=30, timeout_sec=3600) -> SparkJobStatus:
        """
        Blocks until the Dataproc job is completed or failed.

        Args:
            interval_sec (int): Polling interval.
            timeout_sec (int): Timeout limit.

        Returns:
            SparkJobStatus: Latest job status

        Raise:
            SparkJobFailure: Raise error if the job neither failed nor completed within the timeout limit.
        """

        start = time.time()
        while True:
            elapsed_time = time.time() - start
            if timeout_sec and elapsed_time >= timeout_sec:
                raise SparkJobFailure(
                    f"Job is still not completed after {timeout_sec}."
                )

            status = self.get_status()
            if status in [SparkJobStatus.FAILED, SparkJobStatus.COMPLETED]:
                break
            time.sleep(interval_sec)
        return status

    def get_start_time(self):
        return self._job.status.state_start_time

    def get_log_uri(self) -> Optional[str]:
        return (
            f"https://console.cloud.google.com/dataproc/jobs/{self.get_id()}"
            f"?region={self._region}&project={self._project}"
        )


class DataprocRetrievalJob(DataprocJobMixin, RetrievalJob):
    """
    Historical feature retrieval job result for a Dataproc cluster
    """

    def __init__(
        self,
        job: Job,
        refresh_fn: Callable[[], Job],
        cancel_fn: Callable[[], None],
        project: str,
        region: str,
        output_file_uri: str,
    ):
        """
        This is the returned historical feature retrieval job result for DataprocClusterLauncher.

        Args:
            output_file_uri (str): Uri to the historical feature retrieval job output file.
        """
        super().__init__(job, refresh_fn, cancel_fn, project, region)
        self._output_file_uri = output_file_uri

    def get_output_file_uri(self, timeout_sec=None, block=True):
        if not block:
            return self._output_file_uri

        status = self.block_polling(timeout_sec=timeout_sec)
        if status == SparkJobStatus.COMPLETED:
            return self._output_file_uri
        raise SparkJobFailure(self.get_error_message())


class DataprocBatchIngestionJob(DataprocJobMixin, BatchIngestionJob):
    """
    Batch Ingestion job result for a Dataproc cluster
    """

    def get_feature_table(self) -> str:
        return self._job.labels.get(DataprocClusterLauncher.FEATURE_TABLE_LABEL_KEY, "")


class DataprocStreamingIngestionJob(DataprocJobMixin, StreamIngestionJob):
    """
    Streaming Ingestion job result for a Dataproc cluster
    """

    def __init__(
        self,
        job: Job,
        refresh_fn: Callable[[], Job],
        cancel_fn: Callable[[], None],
        project: str,
        region: str,
        job_hash: str,
    ) -> None:
        super().__init__(job, refresh_fn, cancel_fn, project, region)
        self._job_hash = job_hash

    def get_hash(self) -> str:
        return self._job_hash

    def get_feature_table(self) -> str:
        return self._job.labels.get(DataprocClusterLauncher.FEATURE_TABLE_LABEL_KEY, "")


class DataprocClusterLauncher(JobLauncher):
    """
    Submits jobs to an existing Dataproc cluster. Depends on google-cloud-dataproc and
    google-cloud-storage, which are optional dependencies that the user has to installed in
    addition to the Feast SDK.
    """

    EXTERNAL_JARS = ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"]
    JOB_TYPE_LABEL_KEY = "feast_job_type"
    JOB_HASH_LABEL_KEY = "feast_job_hash"
    FEATURE_TABLE_LABEL_KEY = "feast_feature_tables"

    def __init__(
        self,
        cluster_name: str,
        staging_location: str,
        region: str,
        project_id: str,
        executor_instances: str,
        executor_cores: str,
        executor_memory: str,
    ):
        """
        Initialize a dataproc job controller client, used internally for job submission and result
        retrieval.

        Args:
            cluster_name (str):
                Dataproc cluster name.
            staging_location (str):
                GCS directory for the storage of files generated by the launcher, such as the pyspark scripts.
            region (str):
                Dataproc cluster region.
            project_id (str):
                GCP project id for the dataproc cluster.
            executor_instances (str):
                Number of executor instances for dataproc job.
            executor_cores (str):
                Number of cores for dataproc job.
            executor_memory (str):
                Amount of memory for dataproc job.
        """

        self.cluster_name = cluster_name

        scheme, self.staging_bucket, self.remote_path, _, _, _ = urlparse(
            staging_location
        )
        if scheme != "gs":
            raise ValueError(
                "Only GCS staging location is supported for DataprocLauncher."
            )
        self.project_id = project_id
        self.region = region
        self.job_client = JobControllerClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )
        self.executor_instances = executor_instances
        self.executor_cores = executor_cores
        self.executor_memory = executor_memory

    def _stage_file(self, file_path: str, job_id: str) -> str:
        if not os.path.isfile(file_path):
            return file_path

        staging_client = get_staging_client("gs")
        blob_path = os.path.join(
            self.remote_path, job_id, os.path.basename(file_path),
        ).lstrip("/")
        blob_uri_str = f"gs://{self.staging_bucket}/{blob_path}"
        with open(file_path, "rb") as f:
            staging_client.upload_fileobj(
                f, file_path, remote_uri=urlparse(blob_uri_str)
            )

        return blob_uri_str

    def dataproc_submit(
        self, job_params: SparkJobParameters, extra_properties: Dict[str, str]
    ) -> Tuple[Job, Callable[[], Job], Callable[[], None]]:
        local_job_id = str(uuid.uuid4())
        main_file_uri = self._stage_file(job_params.get_main_file_path(), local_job_id)
        job_config: Dict[str, Any] = {
            "reference": {"job_id": local_job_id},
            "placement": {"cluster_name": self.cluster_name},
            "labels": {self.JOB_TYPE_LABEL_KEY: job_params.get_job_type().name.lower()},
        }

        maven_package_properties = {
            "spark.jars.packages": ",".join(job_params.get_extra_packages())
        }
        common_properties = {
            "spark.executor.instances": self.executor_instances,
            "spark.executor.cores": self.executor_cores,
            "spark.executor.memory": self.executor_memory,
        }

        if isinstance(job_params, StreamIngestionJobParameters):
            job_config["labels"][
                self.FEATURE_TABLE_LABEL_KEY
            ] = job_params.get_feature_table_name()
            # Add job hash to labels only for the stream ingestion job
            job_config["labels"][self.JOB_HASH_LABEL_KEY] = job_params.get_job_hash()

        if isinstance(job_params, BatchIngestionJobParameters):
            job_config["labels"][
                self.FEATURE_TABLE_LABEL_KEY
            ] = job_params.get_feature_table_name()

        if job_params.get_class_name():
            scala_job_properties = {
                "spark.yarn.user.classpath.first": "true",
                "spark.executor.instances": self.executor_instances,
                "spark.executor.cores": self.executor_cores,
                "spark.executor.memory": self.executor_memory,
                "spark.pyspark.driver.python": "python3.7",
                "spark.pyspark.python": "python3.7",
            }

            job_config.update(
                {
                    "spark_job": {
                        "jar_file_uris": [main_file_uri] + self.EXTERNAL_JARS,
                        "main_class": job_params.get_class_name(),
                        "args": job_params.get_arguments(),
                        "properties": {
                            **scala_job_properties,
                            **common_properties,
                            **maven_package_properties,
                            **extra_properties,
                        },
                    }
                }
            )
        else:
            job_config.update(
                {
                    "pyspark_job": {
                        "main_python_file_uri": main_file_uri,
                        "jar_file_uris": self.EXTERNAL_JARS,
                        "args": job_params.get_arguments(),
                        "properties": {
                            **common_properties,
                            **maven_package_properties,
                            **extra_properties,
                        },
                    }
                }
            )

        job = self.job_client.submit_job(
            request={
                "project_id": self.project_id,
                "region": self.region,
                "job": job_config,
            }
        )

        refresh_fn = partial(
            self.job_client.get_job,
            project_id=self.project_id,
            region=self.region,
            job_id=job.reference.job_id,
        )
        cancel_fn = partial(self.dataproc_cancel, job.reference.job_id)

        return job, refresh_fn, cancel_fn

    def dataproc_cancel(self, job_id):
        self.job_client.cancel_job(
            project_id=self.project_id, region=self.region, job_id=job_id
        )

    def historical_feature_retrieval(
        self, job_params: RetrievalJobParameters
    ) -> RetrievalJob:
        job, refresh_fn, cancel_fn = self.dataproc_submit(
            job_params, {"dev.feast.outputuri": job_params.get_destination_path()}
        )
        return DataprocRetrievalJob(
            job=job,
            refresh_fn=refresh_fn,
            cancel_fn=cancel_fn,
            project=self.project_id,
            region=self.region,
            output_file_uri=job_params.get_destination_path(),
        )

    def offline_to_online_ingestion(
        self, ingestion_job_params: BatchIngestionJobParameters
    ) -> BatchIngestionJob:
        job, refresh_fn, cancel_fn = self.dataproc_submit(ingestion_job_params, {})
        return DataprocBatchIngestionJob(
            job=job,
            refresh_fn=refresh_fn,
            cancel_fn=cancel_fn,
            project=self.project_id,
            region=self.region,
        )

    def start_stream_to_online_ingestion(
        self, ingestion_job_params: StreamIngestionJobParameters
    ) -> StreamIngestionJob:
        job, refresh_fn, cancel_fn = self.dataproc_submit(ingestion_job_params, {})
        job_hash = ingestion_job_params.get_job_hash()
        return DataprocStreamingIngestionJob(
            job=job,
            refresh_fn=refresh_fn,
            cancel_fn=cancel_fn,
            project=self.project_id,
            region=self.region,
            job_hash=job_hash,
        )

    def get_job_by_id(self, job_id: str) -> SparkJob:
        job = self.job_client.get_job(
            project_id=self.project_id, region=self.region, job_id=job_id
        )
        return self._dataproc_job_to_spark_job(job)

    def _dataproc_job_to_spark_job(self, job: Job) -> SparkJob:
        job_type = job.labels[self.JOB_TYPE_LABEL_KEY]
        job_id = job.reference.job_id
        refresh_fn = partial(
            self.job_client.get_job,
            project_id=self.project_id,
            region=self.region,
            job_id=job_id,
        )
        cancel_fn = partial(self.dataproc_cancel, job_id)

        if job_type == SparkJobType.HISTORICAL_RETRIEVAL.name.lower():
            output_path = job.pyspark_job.properties.get("dev.feast.outputuri", "")
            return DataprocRetrievalJob(
                job=job,
                refresh_fn=refresh_fn,
                cancel_fn=cancel_fn,
                project=self.project_id,
                region=self.region,
                output_file_uri=output_path,
            )

        if job_type == SparkJobType.BATCH_INGESTION.name.lower():
            return DataprocBatchIngestionJob(
                job=job,
                refresh_fn=refresh_fn,
                cancel_fn=cancel_fn,
                project=self.project_id,
                region=self.region,
            )

        if job_type == SparkJobType.STREAM_INGESTION.name.lower():
            job_hash = job.labels[self.JOB_HASH_LABEL_KEY]
            return DataprocStreamingIngestionJob(
                job=job,
                refresh_fn=refresh_fn,
                cancel_fn=cancel_fn,
                project=self.project_id,
                region=self.region,
                job_hash=job_hash,
            )

        raise ValueError(f"Unrecognized job type: {job_type}")

    def list_jobs(self, include_terminated: bool) -> List[SparkJob]:
        job_filter = f"labels.{self.JOB_TYPE_LABEL_KEY} = * AND clusterName = {self.cluster_name}"
        if not include_terminated:
            job_filter = job_filter + "AND status.state = ACTIVE"
        return [
            self._dataproc_job_to_spark_job(job)
            for job in self.job_client.list_jobs(
                project_id=self.project_id, region=self.region, filter=job_filter
            )
        ]
