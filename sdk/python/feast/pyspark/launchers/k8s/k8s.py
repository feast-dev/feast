import random
import string
import time
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, cast
from urllib.parse import urlparse, urlunparse

import yaml
from kubernetes.client.api import CustomObjectsApi

from feast.pyspark.abc import (
    BQ_SPARK_PACKAGE,
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
from feast.staging.storage_client import AbstractStagingClient

from .k8s_utils import (
    DEFAULT_JOB_TEMPLATE,
    HISTORICAL_RETRIEVAL_JOB_TYPE,
    LABEL_FEATURE_TABLE,
    METADATA_JOBHASH,
    METADATA_OUTPUT_URI,
    OFFLINE_TO_ONLINE_JOB_TYPE,
    STREAM_TO_ONLINE_JOB_TYPE,
    JobInfo,
    _cancel_job_by_id,
    _get_api,
    _get_job_by_id,
    _list_jobs,
    _prepare_job_resource,
    _submit_job,
)


def _load_resource_template(job_template_path: Path) -> Dict[str, Any]:
    with open(job_template_path, "rt") as f:
        return yaml.safe_load(f)


def _generate_job_id() -> str:
    return "feast-" + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(8)
    )


class KubernetesJobMixin:
    def __init__(self, api: CustomObjectsApi, namespace: str, job_id: str):
        self._api = api
        self._job_id = job_id
        self._namespace = namespace

    def get_id(self) -> str:
        return self._job_id

    def get_status(self) -> SparkJobStatus:
        job = _get_job_by_id(self._api, self._namespace, self._job_id)
        assert job is not None
        return job.state

    def get_start_time(self) -> datetime:
        job = _get_job_by_id(self._api, self._namespace, self._job_id)
        assert job is not None
        return job.start_time

    def cancel(self):
        _cancel_job_by_id(self._api, self._namespace, self._job_id)

    def _wait_for_complete(self, timeout_seconds: Optional[float]) -> bool:
        """ Returns true if the job completed successfully """
        start_time = time.time()
        while (timeout_seconds is None) or (time.time() - start_time < timeout_seconds):
            status = self.get_status()
            if status == SparkJobStatus.COMPLETED:
                return True
            elif status == SparkJobStatus.FAILED:
                return False
            else:
                time.sleep(1)
        else:
            raise TimeoutError("Timeout waiting for job to complete")


class KubernetesRetrievalJob(KubernetesJobMixin, RetrievalJob):
    """
    Historical feature retrieval job result for a k8s cluster
    """

    def __init__(
        self, api: CustomObjectsApi, namespace: str, job_id: str, output_file_uri: str
    ):
        """
        This is the job object representing the historical retrieval job, returned by KubernetesClusterLauncher.

        Args:
            output_file_uri (str): Uri to the historical feature retrieval job output file.
        """
        super().__init__(api, namespace, job_id)
        self._output_file_uri = output_file_uri

    def get_output_file_uri(self, timeout_sec=None, block=True):
        if not block:
            return self._output_file_uri

        if self._wait_for_complete(timeout_sec):
            return self._output_file_uri
        else:
            raise SparkJobFailure("Spark job failed")


class KubernetesBatchIngestionJob(KubernetesJobMixin, BatchIngestionJob):
    """
    Ingestion job result for a k8s cluster
    """

    def __init__(
        self, api: CustomObjectsApi, namespace: str, job_id: str, feature_table: str
    ):
        super().__init__(api, namespace, job_id)
        self._feature_table = feature_table

    def get_feature_table(self) -> str:
        return self._feature_table


class KubernetesStreamIngestionJob(KubernetesJobMixin, StreamIngestionJob):
    """
    Ingestion streaming job for a k8s cluster
    """

    def __init__(
        self,
        api: CustomObjectsApi,
        namespace: str,
        job_id: str,
        job_hash: str,
        feature_table: str,
    ):
        super().__init__(api, namespace, job_id)
        self._job_hash = job_hash
        self._feature_table = feature_table

    def get_hash(self) -> str:
        return self._job_hash

    def get_feature_table(self) -> str:
        return self._feature_table


class KubernetesJobLauncher(JobLauncher):
    """
    Submits spark jobs to a spark cluster. Currently supports only historical feature retrieval jobs.
    """

    def __init__(
        self,
        namespace: str,
        incluster: bool,
        staging_location: str,
        resource_template_path: Optional[Path],
        staging_client: AbstractStagingClient,
        azure_account_name: str,
        azure_account_key: str,
    ):
        self._namespace = namespace
        self._api = _get_api(incluster=incluster)
        self._staging_location = staging_location
        self._staging_client = staging_client
        self._azure_account_name = azure_account_name
        self._azure_account_key = azure_account_key
        if resource_template_path is not None:
            self._resource_template = _load_resource_template(resource_template_path)
        else:
            self._resource_template = yaml.safe_load(DEFAULT_JOB_TEMPLATE)

    def _job_from_job_info(self, job_info: JobInfo) -> SparkJob:
        if job_info.job_type == HISTORICAL_RETRIEVAL_JOB_TYPE:
            assert METADATA_OUTPUT_URI in job_info.extra_metadata
            return KubernetesRetrievalJob(
                api=self._api,
                namespace=job_info.namespace,
                job_id=job_info.job_id,
                output_file_uri=job_info.extra_metadata[METADATA_OUTPUT_URI],
            )
        elif job_info.job_type == OFFLINE_TO_ONLINE_JOB_TYPE:
            return KubernetesBatchIngestionJob(
                api=self._api,
                namespace=job_info.namespace,
                job_id=job_info.job_id,
                feature_table=job_info.labels.get(LABEL_FEATURE_TABLE, ""),
            )
        elif job_info.job_type == STREAM_TO_ONLINE_JOB_TYPE:
            # job_hash must not be None for stream ingestion jobs
            assert METADATA_JOBHASH in job_info.extra_metadata
            return KubernetesStreamIngestionJob(
                api=self._api,
                namespace=job_info.namespace,
                job_id=job_info.job_id,
                job_hash=job_info.extra_metadata[METADATA_JOBHASH],
                feature_table=job_info.labels.get(LABEL_FEATURE_TABLE, ""),
            )
        else:
            # We should never get here
            raise ValueError(f"Unknown job type {job_info.job_type}")

    def _get_azure_credentials(self):
        uri = urlparse(self._staging_location)
        if uri.scheme != "wasbs":
            return {}
        account_name = self._azure_account_name
        account_key = self._azure_account_key
        if account_name is None or account_key is None:
            raise Exception(
                "Using Azure blob storage requires Azure blob account name and access key to be set in config"
            )
        return {
            f"spark.hadoop.fs.azure.account.key.{account_name}.blob.core.windows.net": f"{account_key}"
        }

    def historical_feature_retrieval(
        self, job_params: RetrievalJobParameters
    ) -> RetrievalJob:
        """
        Submits a historical feature retrieval job to a Spark cluster.

        Raises:
            SparkJobFailure: The spark job submission failed, encountered error
                during execution, or timeout.

        Returns:
            RetrievalJob: wrapper around remote job that returns file uri to the result file.
        """

        with open(job_params.get_main_file_path()) as f:
            pyspark_script = f.read()

        pyspark_script_path = urlunparse(
            self._staging_client.upload_fileobj(
                BytesIO(pyspark_script.encode("utf8")),
                local_path="historical_retrieval.py",
                remote_path_prefix=self._staging_location,
                remote_path_suffix=".py",
            )
        )

        job_id = _generate_job_id()

        resource = _prepare_job_resource(
            job_template=self._resource_template,
            job_id=job_id,
            job_type=HISTORICAL_RETRIEVAL_JOB_TYPE,
            main_application_file=pyspark_script_path,
            main_class=None,
            packages=[],
            jars=[],
            extra_metadata={METADATA_OUTPUT_URI: job_params.get_destination_path()},
            azure_credentials=self._get_azure_credentials(),
            arguments=job_params.get_arguments(),
            namespace=self._namespace,
        )

        job_info = _submit_job(
            api=self._api, resource=resource, namespace=self._namespace,
        )

        return cast(RetrievalJob, self._job_from_job_info(job_info))

    def _upload_jar(self, jar_path: str) -> str:
        if jar_path.startswith("s3://") or jar_path.startswith("s3a://"):
            return jar_path
        elif jar_path.startswith("file://"):
            local_jar_path = urlparse(jar_path).path
        else:
            local_jar_path = jar_path
        with open(local_jar_path, "rb") as f:
            return urlunparse(
                self._staging_client.upload_fileobj(
                    f,
                    local_jar_path,
                    remote_path_prefix=self._staging_location,
                    remote_path_suffix=".jar",
                )
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

        jar_s3_path = self._upload_jar(ingestion_job_params.get_main_file_path())

        job_id = _generate_job_id()

        resource = _prepare_job_resource(
            job_template=self._resource_template,
            job_id=job_id,
            job_type=OFFLINE_TO_ONLINE_JOB_TYPE,
            main_application_file=jar_s3_path,
            main_class=ingestion_job_params.get_class_name(),
            packages=[BQ_SPARK_PACKAGE],
            jars=[],
            extra_metadata={},
            azure_credentials=self._get_azure_credentials(),
            arguments=ingestion_job_params.get_arguments(),
            namespace=self._namespace,
            extra_labels={
                LABEL_FEATURE_TABLE: ingestion_job_params.get_feature_table_name()
            },
        )

        job_info = _submit_job(
            api=self._api, resource=resource, namespace=self._namespace,
        )

        return cast(BatchIngestionJob, self._job_from_job_info(job_info))

    def start_stream_to_online_ingestion(
        self, ingestion_job_params: StreamIngestionJobParameters
    ) -> StreamIngestionJob:
        """
        Starts a stream ingestion job to a Spark cluster.

        Raises:
            SparkJobFailure: The spark job submission failed, encountered error
                during execution, or timeout.

        Returns:
            StreamIngestionJob: wrapper around remote job.
        """

        jar_s3_path = self._upload_jar(ingestion_job_params.get_main_file_path())

        extra_jar_paths: List[str] = []
        for extra_jar in ingestion_job_params.get_extra_jar_paths():
            extra_jar_paths.append(self._upload_jar(extra_jar))

        job_hash = ingestion_job_params.get_job_hash()
        job_id = _generate_job_id()

        resource = _prepare_job_resource(
            job_template=self._resource_template,
            job_id=job_id,
            job_type=STREAM_TO_ONLINE_JOB_TYPE,
            main_application_file=jar_s3_path,
            main_class=ingestion_job_params.get_class_name(),
            packages=[BQ_SPARK_PACKAGE],
            jars=extra_jar_paths,
            extra_metadata={METADATA_JOBHASH: job_hash},
            azure_credentials=self._get_azure_credentials(),
            arguments=ingestion_job_params.get_arguments(),
            namespace=self._namespace,
            extra_labels={
                LABEL_FEATURE_TABLE: ingestion_job_params.get_feature_table_name()
            },
        )

        job_info = _submit_job(
            api=self._api, resource=resource, namespace=self._namespace,
        )

        return cast(StreamIngestionJob, self._job_from_job_info(job_info))

    def get_job_by_id(self, job_id: str) -> SparkJob:
        job_info = _get_job_by_id(self._api, self._namespace, job_id)
        if job_info is None:
            raise KeyError(f"Job iwth id {job_id} not found")
        else:
            return self._job_from_job_info(job_info)

    def list_jobs(self, include_terminated: bool) -> List[SparkJob]:
        return [
            self._job_from_job_info(job)
            for job in _list_jobs(self._api, self._namespace)
            if include_terminated
            or job.state not in (SparkJobStatus.COMPLETED, SparkJobStatus.FAILED)
        ]
