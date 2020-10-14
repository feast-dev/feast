import os
import subprocess
import uuid
from datetime import datetime
from typing import Dict, List

from feast.pyspark.abc import (
    RetrievalJob,
    SparkJobFailure,
    JobLauncher,
    SparkJob,
    SparkJobStatus,
    IngestionJob,
)


class StandaloneClusterSparkJob(SparkJob):
    def __init__(self, job_id: str, **kwargs):
        super().__init__(**kwargs)
        self._job_id = job_id
        self._process = None

    def get_id(self) -> str:
        return self._job_id

    def set_process(self, process: subprocess.Popen):
        self._process = process

    def get_status(self) -> SparkJobStatus:
        code = self._process.poll()
        if code is None:
            return SparkJobStatus.IN_PROGRESS

        if code != 0:
            return SparkJobStatus.FAILED

        return SparkJobStatus.COMPLETED


class StandaloneClusterIngestionJob(IngestionJob, StandaloneClusterSparkJob):
    def __init__(
        self,
        job_id: str,
        feature_table: Dict,
        source: Dict,
        start: datetime,
        end: datetime,
        jar: str,
    ):
        super().__init__(
            job_id=job_id,
            feature_table=feature_table,
            source=source,
            start=start,
            end=end,
            jar=jar,
        )


class StandaloneClusterRetrievalJob(RetrievalJob, StandaloneClusterSparkJob):
    """
    Historical feature retrieval job result for a standalone spark cluster
    """

    def __init__(
        self,
        job_id: str,
        output_file_uri: str,
        feature_tables: List[Dict],
        feature_tables_sources: List[Dict],
        entity_source: Dict,
        destination: Dict,
    ):
        """
        This is the returned historical feature retrieval job result for StandaloneClusterLauncher.

        Args:
            job_id (str): Historical feature retrieval job id.
            process (subprocess.Popen): Pyspark driver process, spawned by the launcher.
            output_file_uri (str): Uri to the historical feature retrieval job output file.
        """
        super().__init__(
            job_id=job_id,
            feature_tables=feature_tables,
            feature_tables_sources=feature_tables_sources,
            entity_source=entity_source,
            destination=destination,
        )
        self._output_file_uri = output_file_uri

    def get_id(self) -> str:
        return self.job_id

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

    def spark_submit(self, job: StandaloneClusterSparkJob) -> subprocess.Popen:
        submission_cmd = [
            self.spark_submit_script_path,
            "--master",
            self.master_url,
            "--name",
            job.get_name(),
        ]

        if job.get_class_name():
            submission_cmd.extend(["--class", job.get_class_name()])

        submission_cmd.append(job.get_main_file_path())
        submission_cmd.extend(job.get_arguments())

        process = subprocess.Popen(submission_cmd)
        job.set_process(process)
        return process

    def historical_feature_retrieval(
        self,
        entity_source_conf: Dict,
        feature_tables_sources_conf: List[Dict],
        feature_tables_conf: List[Dict],
        destination_conf: Dict,
        **kwargs,
    ) -> RetrievalJob:
        job = StandaloneClusterRetrievalJob(
            job_id=str(uuid.uuid4()),
            output_file_uri=destination_conf["path"],
            feature_tables=feature_tables_conf,
            feature_tables_sources=feature_tables_sources_conf,
            entity_source=entity_source_conf,
            destination=destination_conf,
        )

        self.spark_submit(job)
        return job

    def offline_to_online_ingestion(
        self,
        jar_path: str,
        source_conf: Dict,
        feature_table_conf: Dict,
        start: datetime,
        end: datetime,
    ) -> IngestionJob:
        job = StandaloneClusterIngestionJob(
            job_id=str(uuid.uuid4()),
            feature_table=feature_table_conf,
            source=source_conf,
            start=start,
            end=end,
            jar=jar_path,
        )

        self.spark_submit(job)
        return job
