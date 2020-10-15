import abc
from typing import Dict, List


class SparkJobFailure(Exception):
    """
    Job submission failed, encountered error during execution, or timeout
    """

    pass


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


class IngestionJob(SparkJob):
    pass


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
