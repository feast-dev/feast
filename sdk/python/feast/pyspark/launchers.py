import abc
import json
import os
import subprocess
from typing import Dict, List
from urllib.parse import urlparse


class SparkJobFailure(Exception):
    """
    Job submission failed, encountered error during execution, or timeout
    """

    pass


class RetrievalJob(abc.ABC):
    """
    Container for the historical feature retrieval job result
    """

    @abc.abstractmethod
    def get_id(self) -> str:
        """
        Getter for the job id. The job id must be unique for each spark job submission.

        Returns:
            str: Job id.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_output_file_uri(self, timeout_sec=None):
        """
        Get output file uri to the result file. This method will block until the
        job succeeded, or if the job didn't execute successfully within timeout.

        Args:
            timeout_sec (int):
                Max no of seconds to wait until job is done. If "timeout_sec"
                is exceeded or if the job fails, an exception will be raised.

        Raises:
            SparkJobFailure:
                The spark job submission failed, encountered error during execution,
                or timeout.

        Returns:
            str: file uri to the result file.
        """
        raise NotImplementedError


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


class DataprocRetrievalJob(RetrievalJob):
    """
    Historical feature retrieval job result for a Dataproc cluster
    """

    def __init__(self, job_id, operation, output_file_uri):
        """
        This is the returned historical feature retrieval job result for DataprocClusterLauncher.

        Args:
            job_id (str): Historical feature retrieval job id.
            operation (google.api.core.operation.Operation): A Future for the spark job result,
                returned by the dataproc client.
            output_file_uri (str): Uri to the historical feature retrieval job output file.
        """
        self.job_id = job_id
        self._operation = operation
        self._output_file_uri = output_file_uri

    def get_id(self) -> str:
        return self.job_id

    def get_output_file_uri(self, timeout_sec=None):
        try:
            self._operation.result(timeout_sec)
        except Exception as err:
            raise SparkJobFailure(err)
        return self._output_file_uri


class JobLauncher(abc.ABC):
    """
    Submits spark jobs to a spark cluster. Currently supports only historical feature retrieval jobs.
    """

    @abc.abstractmethod
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
        """
        Submits a historical feature retrieval job to a Spark cluster.

        Args:
            pyspark_script (str): Local file path to the pyspark script for historical feature
                retrieval.
            entity_source_conf (Dict): Entity data source configuration.
            feature_tables_sources_conf (List[Dict]): List of feature tables data sources configurations.
            feature_tables_conf (List[Dict]): List of feature table specification.
                The order of the feature table must correspond to that of feature_tables_sources.
            destination_conf (Dict): Retrieval job output destination.
            job_id (str): A job id that is unique for each job submission.

        Raises:
            SparkJobFailure: The spark job submission failed, encountered error
                during execution, or timeout.

        Examples:
            >>> # Entity source from file
            >>> entity_source_conf = {
                    "file": {
                        "format": "parquet",
                        "path": "gs://some-gcs-bucket/customer",
                        "event_timestamp_column": "event_timestamp",
                        "options": {
                            "mergeSchema": "true"
                        } # Optional. Options to be passed to Spark while reading the dataframe from source.
                        "field_mapping": {
                            "id": "customer_id"
                        } # Optional. Map the columns, where the key is the original column name and the value is the new column name.

                    }
                }

            >>> # Entity source from BigQuery
            >>> entity_source_conf = {
                    "bq": {
                        "project": "gcp_project_id",
                        "dataset": "bq_dataset",
                        "table": "customer",
                        "event_timestamp_column": "event_timestamp",
                    }
                }

            >>> feature_table_sources_conf = [
                    {
                        "bq": {
                            "project": "gcp_project_id",
                            "dataset": "bq_dataset",
                            "table": "customer_transactions",
                            "event_timestamp_column": "event_timestamp",
                            "created_timestamp_column": "created_timestamp" # This field is mandatory for feature tables.
                        }
                   },

                   {
                        "file": {
                            "format": "parquet",
                            "path": "gs://some-gcs-bucket/customer_profile",
                            "event_timestamp_column": "event_timestamp",
                            "created_timestamp_column": "created_timestamp",
                            "options": {
                                "mergeSchema": "true"
                            }
                        }
                   },
                ]


            >>> feature_tables_conf = [
                    {
                        "name": "customer_transactions",
                        "entities": [
                            {
                                "name": "customer
                                "type": "int32"
                            }
                        ],
                        "features": [
                            {
                                "name": "total_transactions"
                                "type": "double"
                            },
                            {
                                "name": "total_discounts"
                                "type": "double"
                            }
                        ],
                        "max_age": 86400 # In seconds.
                    },

                    {
                        "name": "customer_profile",
                        "entities": [
                            {
                                "name": "customer
                                "type": "int32"
                            }
                        ],
                        "features": [
                            {
                                "name": "is_vip"
                                "type": "bool"
                            }
                        ],

                    }
                ]

            >>> destination_conf = {
                    "format": "parquet",
                    "path": "gs://some-gcs-bucket/retrieval_output"
                }

        Returns:
            str: file uri to the result file.
        """
        raise NotImplementedError


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


class DataprocClusterLauncher(JobLauncher):
    """
    Submits jobs to an existing Dataproc cluster. Depends on google-cloud-dataproc and
    google-cloud-storage, which are optional dependencies that the user has to installed in
    addition to the Feast SDK.
    """

    def __init__(
        self, cluster_name: str, staging_location: str, region: str, project_id: str,
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
            project_id (str:
                GCP project id for the dataproc cluster.
        """
        from google.cloud import dataproc_v1

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
        self.job_client = dataproc_v1.JobControllerClient(
            client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
        )

    def _stage_files(self, pyspark_script: str, job_id: str) -> str:
        from google.cloud import storage

        client = storage.Client()
        bucket = client.get_bucket(self.staging_bucket)
        blob_path = os.path.join(
            self.remote_path, job_id, os.path.basename(pyspark_script),
        )
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(pyspark_script)

        return f"gs://{self.staging_bucket}/{blob_path}"

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

        pyspark_gcs = self._stage_files(pyspark_script, job_id)
        job = {
            "reference": {"job_id": job_id},
            "placement": {"cluster_name": self.cluster_name},
            "pyspark_job": {
                "main_python_file_uri": pyspark_gcs,
                "args": [
                    "--feature-tables",
                    json.dumps(feature_tables_conf),
                    "--feature-tables-sources",
                    json.dumps(feature_tables_sources_conf),
                    "--entity-source",
                    json.dumps(entity_source_conf),
                    "--destination",
                    json.dumps(destination_conf),
                ],
            },
        }
        operation = self.job_client.submit_job_as_operation(
            request={"project_id": self.project_id, "region": self.region, "job": job}
        )
        output_file = destination_conf["path"]
        return DataprocRetrievalJob(job_id, operation, output_file)
