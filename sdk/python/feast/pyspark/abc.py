import abc
import json
import os
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional


class SparkJobFailure(Exception):
    """
    Job submission failed, encountered error during execution, or timeout
    """

    pass


class SparkJobStatus(Enum):
    IN_PROGRESS = 1
    FAILED = 2
    COMPLETED = 3


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

        :return: SparkJobStatus
        """
        raise NotImplementedError


class SparkJobParameters(abc.ABC):
    @abc.abstractmethod
    def get_name(self) -> str:
        """
        Getter for job name
        :return: Job name
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get_main_file_path(self) -> str:
        """
        Getter for jar | python path
        :return: Full path to file
        """
        raise NotImplementedError

    def get_class_name(self) -> Optional[str]:
        """
        Getter for main class name if it's applicable
        :return: java class path, e.g. feast.ingestion.IngestionJob
        """
        return None

    @abc.abstractmethod
    def get_arguments(self) -> List[str]:
        """
        Getter for job arguments
        E.g., ["--source", '{"kafka":...}', ...]
        :return: List of arguments
        """
        raise NotImplementedError


class RetrievalJobParameters(SparkJobParameters):
    def __init__(
        self,
        feature_tables: List[Dict],
        feature_tables_sources: List[Dict],
        entity_source: Dict,
        destination: Dict,
        **kwargs,
    ):
        self._feature_tables = feature_tables
        self._feature_tables_sources = feature_tables_sources
        self._entity_source = entity_source
        self._destination = destination

    def get_name(self) -> str:
        all_feature_tables_names = [ft["name"] for ft in self._feature_tables]
        return f"HistoryRetrieval-{'-'.join(all_feature_tables_names)}"

    def get_main_file_path(self) -> str:
        return os.path.join(
            os.path.dirname(__file__), "historical_feature_retrieval_job.py"
        )

    def get_arguments(self) -> List[str]:
        return [
            "--feature-tables",
            json.dumps(self._feature_tables),
            "--feature-tables-sources",
            json.dumps(self._feature_tables_sources),
            "--entity-source",
            json.dumps(self._entity_source),
            "--destination",
            json.dumps(self._destination),
        ]


class RetrievalJob(SparkJob):
    """
    Container for the historical feature retrieval job result
    """

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


class IngestionJobParameters(SparkJobParameters):
    def __init__(
        self,
        feature_table: Dict,
        source: Dict,
        start: datetime,
        end: datetime,
        jar: str,
        **kwargs,
    ):
        self._feature_table = feature_table
        self._source = source
        self._start = start
        self._end = end
        self._jar = jar

    def get_name(self) -> str:
        return (
            f"BatchIngestion-{self._feature_table['name']}-"
            f"{self._start.strftime('%Y-%m-%d')}-{self._end.strftime('%Y-%m-%d')}"
        )

    def get_main_file_path(self) -> str:
        return self._jar

    def get_class_name(self) -> Optional[str]:
        return "feast.ingestion.IngestionJob"

    def get_arguments(self) -> List[str]:
        return [
            "--mode",
            "offline",
            "--feature-table",
            json.dumps(self._feature_table),
            "--source",
            json.dumps(self._source),
            "--start",
            self._start.strftime("%Y-%m-%dT%H:%M:%S"),
            "--end",
            self._end.strftime("%Y-%m-%dT%H:%M:%S"),
        ]


class IngestionJob(SparkJob):
    """
    Container for the ingestion job result
    """


class JobLauncher(abc.ABC):
    """
    Submits spark jobs to a spark cluster. Currently supports only historical feature retrieval jobs.
    """

    @abc.abstractmethod
    def historical_feature_retrieval(
        self,
        entity_source_conf: Dict,
        feature_tables_sources_conf: List[Dict],
        feature_tables_conf: List[Dict],
        destination_conf: Dict,
        **kwargs,
    ) -> RetrievalJob:
        """
        Submits a historical feature retrieval job to a Spark cluster.

        Args:
            entity_source_conf (Dict): Entity data source configuration.
            feature_tables_sources_conf (List[Dict]): List of feature tables data sources configurations.
            feature_tables_conf (List[Dict]): List of feature table specification.
                The order of the feature table must correspond to that of feature_tables_sources.
            destination_conf (Dict): Retrieval job output destination.

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

    @abc.abstractmethod
    def offline_to_online_ingestion(
        self,
        jar_path: str,
        source_conf: Dict,
        feature_table_conf: Dict,
        start: datetime,
        end: datetime,
    ) -> IngestionJob:
        """
        Submits a batch ingestion job to a Spark cluster.
        """
        raise NotImplementedError
