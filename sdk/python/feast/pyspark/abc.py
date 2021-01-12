import abc
import hashlib
import json
import os
from base64 import b64encode
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional


class SparkJobFailure(Exception):
    """
    Job submission failed, encountered error during execution, or timeout
    """

    pass


BQ_SPARK_PACKAGE = "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.18.0"


class SparkJobStatus(Enum):
    STARTING = 0
    IN_PROGRESS = 1
    FAILED = 2
    COMPLETED = 3


class SparkJobType(Enum):
    HISTORICAL_RETRIEVAL = 0
    BATCH_INGESTION = 1
    STREAM_INGESTION = 2

    def to_pascal_case(self):
        return self.name.title().replace("_", "")


class SparkJob(abc.ABC):
    """
    Base class for all spark jobs
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
    def get_status(self) -> SparkJobStatus:
        """
        Job Status retrieval

        Returns:
            SparkJobStatus: Job status
        """
        raise NotImplementedError

    @abc.abstractmethod
    def cancel(self):
        """
        Manually terminate job
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_start_time(self) -> datetime:
        """
        Get job start time.
        """

    def get_log_uri(self) -> Optional[str]:
        """
        Get path to Spark job log, if applicable.
        """
        return None


class SparkJobParameters(abc.ABC):
    @abc.abstractmethod
    def get_name(self) -> str:
        """
        Getter for job name
        Returns:
            str: Job name.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_job_type(self) -> SparkJobType:
        """
        Getter for job type.
        Returns:
            SparkJobType: Job type enum.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_main_file_path(self) -> str:
        """
        Getter for jar | python path
        Returns:
            str: Full path to file.
        """
        raise NotImplementedError

    def get_class_name(self) -> Optional[str]:
        """
        Getter for main class name if it's applicable
        Returns:
            Optional[str]: java class path, e.g. feast.ingestion.IngestionJob.
        """
        return None

    def get_extra_packages(self) -> List[str]:
        """
        Getter for extra maven packages to be included on driver and executor
        classpath if applicable.
        Returns:
            List[str]: List of maven packages
        """
        return []

    @abc.abstractmethod
    def get_arguments(self) -> List[str]:
        """
        Getter for job arguments
        E.g., ["--source", '{"kafka":...}', ...]
        Returns:
            List[str]: List of arguments.
        """
        raise NotImplementedError


class RetrievalJobParameters(SparkJobParameters):
    def __init__(
        self,
        feature_tables: List[Dict],
        feature_tables_sources: List[Dict],
        entity_source: Dict,
        destination: Dict,
        extra_packages: Optional[List[str]] = None,
    ):
        """
        Args:
            entity_source (Dict): Entity data source configuration.
            feature_tables_sources (List[Dict]): List of feature tables data sources configurations.
            feature_tables (List[Dict]): List of feature table specification.
                The order of the feature table must correspond to that of feature_tables_sources.
            destination (Dict): Retrieval job output destination.
            extra_packages (Optional[List[str]): Extra maven packages to be included on Spark driver
                and executors classpath.

        Examples:
            >>> # Entity source from file
            >>> entity_source = {
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
            >>> entity_source = {
                    "bq": {
                        "project": "gcp_project_id",
                        "dataset": "bq_dataset",
                        "table": "customer",
                        "event_timestamp_column": "event_timestamp",
                    }
                }

            >>> feature_tables_sources = [
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


            >>> feature_tables = [
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

            >>> destination = {
                    "format": "parquet",
                    "path": "gs://some-gcs-bucket/retrieval_output"
                }

        """
        self._feature_tables = feature_tables
        self._feature_tables_sources = feature_tables_sources
        self._entity_source = entity_source
        self._destination = destination
        self._extra_packages = extra_packages if extra_packages else []

    def get_name(self) -> str:
        all_feature_tables_names = [ft["name"] for ft in self._feature_tables]
        return f"{self.get_job_type().to_pascal_case()}-{'-'.join(all_feature_tables_names)}"

    def get_job_type(self) -> SparkJobType:
        return SparkJobType.HISTORICAL_RETRIEVAL

    def get_main_file_path(self) -> str:
        return os.path.join(
            os.path.dirname(__file__), "historical_feature_retrieval_job.py"
        )

    def get_extra_packages(self) -> List[str]:
        return self._extra_packages

    def get_arguments(self) -> List[str]:
        def json_b64_encode(obj) -> str:
            return b64encode(json.dumps(obj).encode("utf8")).decode("ascii")

        return [
            "--feature-tables",
            json_b64_encode(self._feature_tables),
            "--feature-tables-sources",
            json_b64_encode(self._feature_tables_sources),
            "--entity-source",
            json_b64_encode(self._entity_source),
            "--destination",
            json_b64_encode(self._destination),
        ]

    def get_destination_path(self) -> str:
        return self._destination["path"]


class RetrievalJob(SparkJob):
    """
    Container for the historical feature retrieval job result
    """

    @abc.abstractmethod
    def get_output_file_uri(self, timeout_sec=None, block=True):
        """
        Get output file uri to the result file. This method will block until the
        job succeeded, or if the job didn't execute successfully within timeout.

        Args:
            timeout_sec (int):
                Max no of seconds to wait until job is done. If "timeout_sec"
                is exceeded or if the job fails, an exception will be raised.
            block (bool):
                If false, don't block until the job is done. If block=True, timeout parameter is
                ignored.
        Raises:
            SparkJobFailure:
                The spark job submission failed, encountered error during execution,
                or timeout.

        Returns:
            str: file uri to the result file.
        """
        raise NotImplementedError


class IngestionJobParameters(SparkJobParameters):
    def __init__(
        self,
        feature_table: Dict,
        source: Dict,
        jar: str,
        redis_host: str,
        redis_port: int,
        redis_ssl: bool,
        statsd_host: Optional[str] = None,
        statsd_port: Optional[int] = None,
        deadletter_path: Optional[str] = None,
        stencil_url: Optional[str] = None,
        drop_invalid_rows: bool = False,
    ):
        self._feature_table = feature_table
        self._source = source
        self._jar = jar
        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_ssl = redis_ssl
        self._statsd_host = statsd_host
        self._statsd_port = statsd_port
        self._deadletter_path = deadletter_path
        self._stencil_url = stencil_url
        self._drop_invalid_rows = drop_invalid_rows

    def _get_redis_config(self):
        return dict(host=self._redis_host, port=self._redis_port, ssl=self._redis_ssl)

    def _get_statsd_config(self):
        return (
            dict(host=self._statsd_host, port=self._statsd_port)
            if self._statsd_host
            else None
        )

    def get_feature_table_name(self) -> str:
        return self._feature_table["name"]

    def get_main_file_path(self) -> str:
        return self._jar

    def get_class_name(self) -> Optional[str]:
        return "feast.ingestion.IngestionJob"

    def get_arguments(self) -> List[str]:
        args = [
            "--feature-table",
            json.dumps(self._feature_table),
            "--source",
            json.dumps(self._source),
            "--redis",
            json.dumps(self._get_redis_config()),
        ]

        if self._get_statsd_config():
            args.extend(["--statsd", json.dumps(self._get_statsd_config())])

        if self._deadletter_path:
            args.extend(
                [
                    "--deadletter-path",
                    os.path.join(self._deadletter_path, self.get_feature_table_name()),
                ]
            )

        if self._stencil_url:
            args.extend(["--stencil-url", self._stencil_url])

        if self._drop_invalid_rows:
            args.extend(["--drop-invalid"])

        return args


class BatchIngestionJobParameters(IngestionJobParameters):
    def __init__(
        self,
        feature_table: Dict,
        source: Dict,
        start: datetime,
        end: datetime,
        jar: str,
        redis_host: str,
        redis_port: int,
        redis_ssl: bool,
        statsd_host: Optional[str] = None,
        statsd_port: Optional[int] = None,
        deadletter_path: Optional[str] = None,
        stencil_url: Optional[str] = None,
    ):
        super().__init__(
            feature_table,
            source,
            jar,
            redis_host,
            redis_port,
            redis_ssl,
            statsd_host,
            statsd_port,
            deadletter_path,
            stencil_url,
        )
        self._start = start
        self._end = end

    def get_name(self) -> str:
        return (
            f"{self.get_job_type().to_pascal_case()}-{self.get_feature_table_name()}-"
            f"{self._start.strftime('%Y-%m-%d')}-{self._end.strftime('%Y-%m-%d')}"
        )

    def get_job_type(self) -> SparkJobType:
        return SparkJobType.BATCH_INGESTION

    def get_arguments(self) -> List[str]:
        return super().get_arguments() + [
            "--mode",
            "offline",
            "--start",
            self._start.strftime("%Y-%m-%dT%H:%M:%S"),
            "--end",
            self._end.strftime("%Y-%m-%dT%H:%M:%S"),
        ]


class StreamIngestionJobParameters(IngestionJobParameters):
    def __init__(
        self,
        feature_table: Dict,
        source: Dict,
        jar: str,
        extra_jars: List[str],
        redis_host: str,
        redis_port: int,
        redis_ssl: bool,
        statsd_host: Optional[str] = None,
        statsd_port: Optional[int] = None,
        deadletter_path: Optional[str] = None,
        stencil_url: Optional[str] = None,
        drop_invalid_rows: bool = False,
    ):
        super().__init__(
            feature_table,
            source,
            jar,
            redis_host,
            redis_port,
            redis_ssl,
            statsd_host,
            statsd_port,
            deadletter_path,
            stencil_url,
            drop_invalid_rows,
        )
        self._extra_jars = extra_jars

    def get_name(self) -> str:
        return f"{self.get_job_type().to_pascal_case()}-{self.get_feature_table_name()}"

    def get_job_type(self) -> SparkJobType:
        return SparkJobType.STREAM_INGESTION

    def get_extra_jar_paths(self) -> List[str]:
        return self._extra_jars

    def get_arguments(self) -> List[str]:
        return super().get_arguments() + [
            "--mode",
            "online",
        ]

    def get_job_hash(self) -> str:
        job_json = json.dumps(
            {"source": self._source, "feature_table": self._feature_table},
            sort_keys=True,
        )
        return hashlib.md5(job_json.encode()).hexdigest()


class BatchIngestionJob(SparkJob):
    """
    Container for the ingestion job result
    """

    @abc.abstractmethod
    def get_feature_table(self) -> str:
        """
        Get the feature table name associated with this job. Return empty string if unable to
        determine the feature table, such as when the job is created by the earlier
        version of Feast.

        Returns:
            str: Feature table name
        """
        raise NotImplementedError


class StreamIngestionJob(SparkJob):
    """
    Container for the streaming ingestion job result
    """

    def get_hash(self) -> str:
        """Gets the consistent hash of this stream ingestion job.

        The hash needs to be persisted at the data processing layer, so that we can get the same
        hash when retrieving the job from Spark.

        Returns:
            str: The hash for this streaming ingestion job
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_feature_table(self) -> str:
        """
        Get the feature table name associated with this job. Return `None` if unable to
        determine the feature table, such as when the job is created by the earlier
        version of Feast.

        Returns:
            str: Feature table name
        """
        raise NotImplementedError


class JobLauncher(abc.ABC):
    """
    Submits spark jobs to a spark cluster. Currently supports only historical feature retrieval jobs.
    """

    @abc.abstractmethod
    def historical_feature_retrieval(
        self, retrieval_job_params: RetrievalJobParameters
    ) -> RetrievalJob:
        """
        Submits a historical feature retrieval job to a Spark cluster.

        Raises:
            SparkJobFailure: The spark job submission failed, encountered error
                during execution, or timeout.

        Returns:
            RetrievalJob: wrapper around remote job that returns file uri to the result file.
        """
        raise NotImplementedError

    @abc.abstractmethod
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
        raise NotImplementedError

    @abc.abstractmethod
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
        raise NotImplementedError

    @abc.abstractmethod
    def get_job_by_id(self, job_id: str) -> SparkJob:
        raise NotImplementedError

    @abc.abstractmethod
    def list_jobs(self, include_terminated: bool) -> List[SparkJob]:
        raise NotImplementedError
