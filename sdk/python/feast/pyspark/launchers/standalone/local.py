import os
import subprocess
import uuid

from feast.pyspark.abc import (
    BatchIngestionJob,
    BatchIngestionJobParameters,
    JobLauncher,
    RetrievalJob,
    RetrievalJobParameters,
    SparkJobFailure,
    SparkJobParameters,
    SparkJobStatus,
    StreamIngestionJob,
    StreamIngestionJobParameters,
)


class StandaloneClusterJobMixin:
    def __init__(self, job_id: str, process: subprocess.Popen):
        self._job_id = job_id
        self._process = process

    def get_id(self) -> str:
        return self._job_id

    def get_status(self) -> SparkJobStatus:
        code = self._process.poll()
        if code is None:
            return SparkJobStatus.IN_PROGRESS

        if code != 0:
            return SparkJobStatus.FAILED

        return SparkJobStatus.COMPLETED


class StandaloneClusterBatchIngestionJob(StandaloneClusterJobMixin, BatchIngestionJob):
    """
    Ingestion job result for a standalone spark cluster
    """

    pass


class StandaloneClusterRetrievalJob(StandaloneClusterJobMixin, RetrievalJob):
    """
    Historical feature retrieval job result for a standalone spark cluster
    """

    def __init__(self, job_id: str, process: subprocess.Popen, output_file_uri: str):
        """
        This is the returned historical feature retrieval job result for StandaloneClusterLauncher.

        Args:
            job_id (str): Historical feature retrieval job id.
            process (subprocess.Popen): Pyspark driver process, spawned by the launcher.
            output_file_uri (str): Uri to the historical feature retrieval job output file.
        """
        super().__init__(job_id, process)
        self._output_file_uri = output_file_uri

    def get_output_file_uri(self, timeout_sec: int = None):
        with self._process as p:
            try:
                p.wait(timeout_sec)
            except Exception:
                p.kill()
                raise SparkJobFailure("Timeout waiting for subprocess to return")
        if self._process.returncode != 0:
            stderr = "" if self._process.stderr is None else self._process.stderr.read()
            stdout = "" if self._process.stdout is None else self._process.stdout.read()

            raise SparkJobFailure(
                f"Non zero return code: {self._process.returncode}. stderr: {stderr} stdout: {stdout}"
            )


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

    def spark_submit(self, job_params: SparkJobParameters) -> subprocess.Popen:
        submission_cmd = [
            self.spark_submit_script_path,
            "--master",
            self.master_url,
            "--name",
            job_params.get_name(),
        ]

        if job_params.get_class_name():
            submission_cmd.extend(["--class", job_params.get_class_name()])

        submission_cmd.append(job_params.get_main_file_path())
        submission_cmd.extend(job_params.get_arguments())

        return subprocess.Popen(submission_cmd)

    def historical_feature_retrieval(
        self, job_params: RetrievalJobParameters
    ) -> RetrievalJob:
        job_id = str(uuid.uuid4())
        return StandaloneClusterRetrievalJob(
            job_id, self.spark_submit(job_params), job_params.get_destination_path()
        )

    def offline_to_online_ingestion(
        self, job_params: BatchIngestionJobParameters
    ) -> BatchIngestionJob:
        job_id = str(uuid.uuid4())
        return StandaloneClusterBatchIngestionJob(job_id, self.spark_submit(job_params))

    def start_stream_to_online_ingestion(
        self, ingestion_job_params: StreamIngestionJobParameters
    ) -> StreamIngestionJob:
        raise NotImplementedError

    def stage_dataframe(
        self, df, event_timestamp_column: str, created_timestamp_column: str,
    ):
        raise NotImplementedError
