import os
import socket
import subprocess
import threading
import uuid
from contextlib import closing
from datetime import datetime
from typing import Dict, List, Optional

import requests
from requests.exceptions import RequestException

from feast.pyspark.abc import (
    BQ_SPARK_PACKAGE,
    BatchIngestionJob,
    BatchIngestionJobParameters,
    JobLauncher,
    RetrievalJob,
    RetrievalJobParameters,
    SparkJob,
    SparkJobFailure,
    SparkJobParameters,
    SparkJobStatus,
    StreamIngestionJob,
    StreamIngestionJobParameters,
)


class JobCache:
    """
    A *global* in-memory cache of Spark jobs.

    This is necessary since we can't easily keep track of running Spark jobs in local mode, since
    there is no external state (unlike EMR and Dataproc which keep track of the running jobs for
    us).
    """

    # Map of job_id -> spark job
    job_by_id: Dict[str, SparkJob]

    # Map of job_id -> job_hash. The value can be None, indicating this job was
    # manually created and Job Service isn't maintaining the state of this job
    hash_by_id: Dict[str, Optional[str]]

    # This reentrant lock is necessary for multi-threading access
    lock: threading.RLock

    def __init__(self):
        self.job_by_id = {}
        self.hash_by_id = {}
        self.lock = threading.RLock()

    def add_job(self, job: SparkJob) -> None:
        """Add a Spark job to the cache.

        Args:
            job (SparkJob): The new Spark job to add.
        """
        with self.lock:
            self.job_by_id[job.get_id()] = job
            if isinstance(job, StreamIngestionJob):
                self.hash_by_id[job.get_id()] = job.get_hash()

    def list_jobs(self) -> List[SparkJob]:
        """List all Spark jobs in the cache."""
        with self.lock:
            return list(self.job_by_id.values())

    def get_job_by_id(self, job_id: str) -> SparkJob:
        """Get a Spark job with the given ID. Throws an exception if such job doesn't exist.

        Args:
            job_id (str): External ID of the Spark job to get.

        Returns:
            SparkJob: The Spark job with the given ID.
        """
        with self.lock:
            return self.job_by_id[job_id]


global_job_cache = JobCache()


def reset_job_cache():
    global global_job_cache
    global_job_cache = JobCache()


def _find_free_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


class StandaloneClusterJobMixin:
    def __init__(
        self, job_id: str, job_name: str, process: subprocess.Popen, ui_port: int = None
    ):
        self._job_id = job_id
        self._job_name = job_name
        self._process = process
        self._ui_port = ui_port
        self._start_time = datetime.utcnow()

    def get_id(self) -> str:
        return self._job_id

    def check_if_started(self):
        if not self._ui_port:
            return True

        try:
            applications = requests.get(
                f"http://localhost:{self._ui_port}/api/v1/applications"
            ).json()
        except RequestException:
            return False

        app = next(
            iter(app for app in applications if app["name"] == self._job_name), None
        )
        if not app:
            return False

        stages = requests.get(
            f"http://localhost:{self._ui_port}/api/v1/applications/{app['id']}/stages"
        ).json()
        return bool(stages)

    def get_start_time(self) -> datetime:
        return self._start_time

    def get_status(self) -> SparkJobStatus:
        code = self._process.poll()
        if code is None:
            if not self.check_if_started():
                return SparkJobStatus.STARTING

            return SparkJobStatus.IN_PROGRESS

        if code != 0:
            return SparkJobStatus.FAILED

        return SparkJobStatus.COMPLETED

    def cancel(self):
        self._process.terminate()


class StandaloneClusterBatchIngestionJob(StandaloneClusterJobMixin, BatchIngestionJob):
    """
    Batch Ingestion job result for a standalone spark cluster
    """

    def __init__(
        self,
        job_id: str,
        job_name: str,
        process: subprocess.Popen,
        ui_port: int,
        feature_table: str,
    ) -> None:
        super().__init__(job_id, job_name, process, ui_port)
        self._feature_table = feature_table

    def get_feature_table(self) -> str:
        return self._feature_table


class StandaloneClusterStreamingIngestionJob(
    StandaloneClusterJobMixin, StreamIngestionJob
):
    """
    Streaming Ingestion job result for a standalone spark cluster
    """

    def __init__(
        self,
        job_id: str,
        job_name: str,
        process: subprocess.Popen,
        ui_port: int,
        job_hash: str,
        feature_table: str,
    ) -> None:
        super().__init__(job_id, job_name, process, ui_port)
        self._job_hash = job_hash
        self._feature_table = feature_table

    def get_hash(self) -> str:
        return self._job_hash

    def get_feature_table(self) -> str:
        return self._feature_table


class StandaloneClusterRetrievalJob(StandaloneClusterJobMixin, RetrievalJob):
    """
    Historical feature retrieval job result for a standalone spark cluster
    """

    def __init__(
        self,
        job_id: str,
        job_name: str,
        process: subprocess.Popen,
        output_file_uri: str,
    ):
        """
        This is the returned historical feature retrieval job result for StandaloneClusterLauncher.

        Args:
            job_id (str): Historical feature retrieval job id.
            process (subprocess.Popen): Pyspark driver process, spawned by the launcher.
            output_file_uri (str): Uri to the historical feature retrieval job output file.
        """
        super().__init__(job_id, job_name, process)
        self._output_file_uri = output_file_uri

    def get_output_file_uri(self, timeout_sec: int = None, block=True):
        if not block:
            return self._output_file_uri

        with self._process as p:
            try:
                p.wait(timeout_sec)
                return self._output_file_uri
            except Exception:
                p.kill()
                raise SparkJobFailure("Timeout waiting for subprocess to return")
        if self._process.returncode != 0:
            stderr = "" if self._process.stderr is None else self._process.stderr.read()
            stdout = "" if self._process.stdout is None else self._process.stdout.read()

            raise SparkJobFailure(
                f"Non zero return code: {self._process.returncode}. stderr: {stderr} stdout: {stdout}"
            )
        return self._output_file_uri


class StandaloneClusterLauncher(JobLauncher):
    """
    Submits jobs to a standalone Spark cluster in client mode.
    """

    def __init__(self, master_url: str, spark_home: str = None):
        """
        This launcher executes the spark-submit script in a subprocess. The subprocess
        will run until the Pyspark driver exits.

        Args:
            master_url (str):
                Spark cluster url. Must start with spark://.
            spark_home (str):
                Local file path to Spark installation directory. If not provided,
                the environmental variable `SPARK_HOME` will be used instead.
        """
        self.master_url = master_url
        self.spark_home = spark_home if spark_home else os.getenv("SPARK_HOME")

    @property
    def spark_submit_script_path(self):
        return os.path.join(self.spark_home, "bin/spark-submit")

    def spark_submit(
        self, job_params: SparkJobParameters, ui_port: int = None
    ) -> subprocess.Popen:
        submission_cmd = [
            self.spark_submit_script_path,
            "--master",
            self.master_url,
            "--name",
            job_params.get_name(),
        ]

        if job_params.get_class_name():
            submission_cmd.extend(["--class", job_params.get_class_name()])

        if ui_port:
            submission_cmd.extend(["--conf", f"spark.ui.port={ui_port}"])

        # Workaround for https://github.com/apache/spark/pull/26552
        # Fix running spark job with bigquery connector (w/ shadowing) on JDK 9+
        submission_cmd.extend(
            [
                "--conf",
                "spark.executor.extraJavaOptions="
                "-Dcom.google.cloud.spark.bigquery.repackaged.io.netty.tryReflectionSetAccessible=true -Duser.timezone=GMT",
                "--conf",
                "spark.driver.extraJavaOptions="
                "-Dcom.google.cloud.spark.bigquery.repackaged.io.netty.tryReflectionSetAccessible=true -Duser.timezone=GMT",
                "--conf",
                "spark.sql.session.timeZone=UTC",  # ignore local timezone
                "--packages",
                ",".join([BQ_SPARK_PACKAGE] + job_params.get_extra_packages()),
                "--jars",
                "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop2-latest.jar,"
                "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/2.7.3/hadoop-aws-2.7.3.jar,"
                "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.7.4/aws-java-sdk-1.7.4.jar",
                "--conf",
                "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
                "--conf",
                "spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            ]
        )

        submission_cmd.append(job_params.get_main_file_path())
        submission_cmd.extend(job_params.get_arguments())

        return subprocess.Popen(submission_cmd)

    def historical_feature_retrieval(
        self, job_params: RetrievalJobParameters
    ) -> RetrievalJob:
        job_id = str(uuid.uuid4())
        job = StandaloneClusterRetrievalJob(
            job_id,
            job_params.get_name(),
            self.spark_submit(job_params),
            job_params.get_destination_path(),
        )
        global_job_cache.add_job(job)
        return job

    def offline_to_online_ingestion(
        self, ingestion_job_params: BatchIngestionJobParameters
    ) -> BatchIngestionJob:
        job_id = str(uuid.uuid4())
        ui_port = _find_free_port()
        job = StandaloneClusterBatchIngestionJob(
            job_id,
            ingestion_job_params.get_name(),
            self.spark_submit(ingestion_job_params, ui_port),
            ui_port,
            ingestion_job_params.get_feature_table_name(),
        )
        global_job_cache.add_job(job)
        return job

    def start_stream_to_online_ingestion(
        self, ingestion_job_params: StreamIngestionJobParameters
    ) -> StreamIngestionJob:
        job_id = str(uuid.uuid4())
        ui_port = _find_free_port()
        job = StandaloneClusterStreamingIngestionJob(
            job_id,
            ingestion_job_params.get_name(),
            self.spark_submit(ingestion_job_params, ui_port),
            ui_port,
            ingestion_job_params.get_job_hash(),
            ingestion_job_params.get_feature_table_name(),
        )
        global_job_cache.add_job(job)
        return job

    def get_job_by_id(self, job_id: str) -> SparkJob:
        return global_job_cache.get_job_by_id(job_id)

    def list_jobs(self, include_terminated: bool) -> List[SparkJob]:
        if include_terminated is True:
            return global_job_cache.list_jobs()
        else:
            return [
                job
                for job in global_job_cache.list_jobs()
                if job.get_status()
                in (SparkJobStatus.STARTING, SparkJobStatus.IN_PROGRESS)
            ]
