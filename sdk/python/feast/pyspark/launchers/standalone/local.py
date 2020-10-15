import json
import os
import subprocess
from typing import Dict, List

from feast.pyspark.abc import JobLauncher, RetrievalJob, SparkJobFailure


class StandaloneClusterRetrievalJob(RetrievalJob):
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
        self.job_id = job_id
        self._process = process
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

    def historical_feature_retrieval(
        self,
        pyspark_script: str,
        entity_source_conf: Dict,
        feature_tables_sources_conf: List[Dict],
        feature_tables_conf: List[Dict],
        destination_conf: Dict,
        job_id: str,
        **kwargs,
    ) -> RetrievalJob:

        submission_cmd = [
            self.spark_submit_script_path,
            "--master",
            self.master_url,
            "--name",
            job_id,
            pyspark_script,
            "--feature-tables",
            json.dumps(feature_tables_conf),
            "--feature-tables-sources",
            json.dumps(feature_tables_sources_conf),
            "--entity-source",
            json.dumps(entity_source_conf),
            "--destination",
            json.dumps(destination_conf),
        ]

        process = subprocess.Popen(submission_cmd, shell=True)
        output_file = destination_conf["path"]
        return StandaloneClusterRetrievalJob(job_id, process, output_file)
