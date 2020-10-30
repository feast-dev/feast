import os
import socket
import subprocess
import uuid
from contextlib import closing
from typing import List

import requests
from requests.exceptions import RequestException

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
    StreamIngestionJob,
    StreamIngestionJobParameters,
)

# In-memory cache of Spark jobs
# This is necessary since we can't query Spark jobs in local mode
JOB_CACHE = {}


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

    pass


class StandaloneClusterStreamingIngestionJob(
    StandaloneClusterJobMixin, StreamIngestionJob
):
    """
    Streaming Ingestion job result for a standalone spark cluster
    """

    pass


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

        if job_params.get_extra_options():
            submission_cmd.extend(job_params.get_extra_options().split(" "))

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
        JOB_CACHE[job_id] = job  # type: ignore
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
        )
        JOB_CACHE[job_id] = job  # type: ignore
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
        )
        JOB_CACHE[job_id] = job  # type: ignore
        return job

    def stage_dataframe(self, df, event_timestamp_column: str):
        raise NotImplementedError

    def get_job_by_id(self, job_id: str) -> SparkJob:
        return JOB_CACHE[job_id]

    def list_jobs(self, include_terminated: bool) -> List[SparkJob]:
        return list(JOB_CACHE.values())
