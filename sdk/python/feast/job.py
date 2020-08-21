from typing import List
from urllib.parse import urlparse

import fastavro
import grpc
import pandas as pd

from feast.constants import CONFIG_TIMEOUT_KEY
from feast.constants import FEAST_DEFAULT_OPTIONS as defaults
from feast.serving.ServingService_pb2 import (
    DATA_FORMAT_AVRO,
    JOB_STATUS_DONE,
    GetJobRequest,
)
from feast.serving.ServingService_pb2 import Job as JobProto
from feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.staging.storage_client import get_staging_client
from feast.wait import wait_retry_backoff
from tensorflow_metadata.proto.v0 import statistics_pb2

# Maximum no of seconds to wait until the retrieval jobs status is DONE in Feast
# Currently set to the maximum query execution time limit in BigQuery
DEFAULT_TIMEOUT_SEC: int = 21600

# Maximum no of seconds to wait before reloading the job status in Feast
MAX_WAIT_INTERVAL_SEC: int = 60


class RetrievalJob:
    """
    A class representing a job for feature retrieval in Feast.
    """

    def __init__(
        self,
        job_proto: JobProto,
        serving_stub: ServingServiceStub,
        auth_metadata_plugin: grpc.AuthMetadataPlugin = None,
    ):
        """
        Args:
            job_proto: Job proto object (wrapped by this job object)
            serving_stub: Stub for Feast serving service
            auth_metadata_plugin: plugin to fetch auth metadata
        """
        self.job_proto = job_proto
        self.serving_stub = serving_stub
        self.auth_metadata = auth_metadata_plugin

    @property
    def id(self):
        """
        Getter for the Job Id
        """
        return self.job_proto.id

    @property
    def status(self):
        """
        Getter for the Job status from Feast Core
        """
        return self.job_proto.status

    def reload(self):
        """
        Reload the latest job status
        Returns: None
        """
        self.job_proto = self.serving_stub.GetJob(
            GetJobRequest(job=self.job_proto),
            metadata=self.auth_metadata.get_signed_meta() if self.auth_metadata else (),
        ).job

    def get_avro_files(self, timeout_sec: int = int(defaults[CONFIG_TIMEOUT_KEY])):
        """
        Wait until job is done to get the file uri to Avro result files on
        Google Cloud Storage.

        Args:
            timeout_sec (int):
                Max no of seconds to wait until job is done. If "timeout_sec"
                is exceeded, an exception will be raised.

        Returns:
            str: Google Cloud Storage file uris of the returned Avro files.
        """

        def try_retrieve():
            self.reload()
            return None, self.status == JOB_STATUS_DONE

        wait_retry_backoff(
            retry_fn=try_retrieve,
            timeout_secs=timeout_sec,
            timeout_msg="Timeout exceeded while waiting for result. Please retry "
            "this method or use a longer timeout value.",
        )

        if self.job_proto.error:
            raise Exception(self.job_proto.error)

        if self.job_proto.data_format != DATA_FORMAT_AVRO:
            raise Exception(
                "Feast only supports Avro data format for now. Please check "
                "your Feast Serving deployment."
            )

        return [urlparse(uri) for uri in self.job_proto.file_uris]

    def result(self, timeout_sec: int = int(defaults[CONFIG_TIMEOUT_KEY])):
        """
        Wait until job is done to get an iterable rows of result. The row can
        only represent an Avro row in Feast 0.3.

        Args:
            timeout_sec (int):
                Max no of seconds to wait until job is done. If "timeout_sec"
                is exceeded, an exception will be raised.

        Returns:
            Iterable of Avro rows.
        """
        uris = self.get_avro_files(timeout_sec)
        for file_uri in uris:
            file_obj = get_staging_client(file_uri.scheme).download_file(file_uri)
            file_obj.seek(0)
            avro_reader = fastavro.reader(file_obj)

            for record in avro_reader:
                yield record

    def to_dataframe(
        self, timeout_sec: int = int(defaults[CONFIG_TIMEOUT_KEY])
    ) -> pd.DataFrame:
        """
        Wait until a job is done to get an iterable rows of result. This method
        will split the response into chunked DataFrame of a specified size to
        to be yielded to the instance calling it.

        Args:
            max_chunk_size (int):
                Maximum number of rows that the DataFrame should contain.

            timeout_sec (int):
                Max no of seconds to wait until job is done. If "timeout_sec"
                is exceeded, an exception will be raised.

        Returns:
            pd.DataFrame:
                Pandas DataFrame of the feature values.
        """
        records = [r for r in self.result(timeout_sec=timeout_sec)]
        return pd.DataFrame.from_records(records)

    def to_chunked_dataframe(
        self,
        max_chunk_size: int = -1,
        timeout_sec: int = int(defaults[CONFIG_TIMEOUT_KEY]),
    ) -> pd.DataFrame:
        """
        Wait until a job is done to get an iterable rows of result. This method
        will split the response into chunked DataFrame of a specified size to
        to be yielded to the instance calling it.

        Args:
            max_chunk_size (int):
                Maximum number of rows that the DataFrame should contain.

            timeout_sec (int):
                Max no of seconds to wait until job is done. If "timeout_sec"
                is exceeded, an exception will be raised.

        Returns:
            pd.DataFrame:
                Pandas DataFrame of the feature values.
        """

        # Object is Avro row type object, refer to self.result function for this type
        records: List[dict] = []

        # Max chunk size defined by user
        for result in self.result(timeout_sec=timeout_sec):
            result.append(records)
            if len(records) == max_chunk_size:
                df = pd.DataFrame.from_records(records)
                records.clear()  # Empty records array
                yield df

        # Handle for last chunk that is < max_chunk_size
        if not records:
            yield pd.DataFrame.from_records(records)

    def __iter__(self):
        return iter(self.result())

    def statistics(
        self, timeout_sec: int = int(defaults[CONFIG_TIMEOUT_KEY])
    ) -> statistics_pb2.DatasetFeatureStatisticsList:
        """
        Get statistics computed over the retrieved data set. Statistics will only be computed for
        columns that are part of Feast, and not the columns that were provided.

        Args:
            timeout_sec (int):
                Max no of seconds to wait until job is done. If "timeout_sec"
                is exceeded, an exception will be raised.

        Returns:
            DatasetFeatureStatisticsList containing statistics of Feast features over the retrieved dataset.
        """
        self.get_avro_files(timeout_sec)  # wait for job completion
        if self.job_proto.error:
            raise Exception(self.job_proto.error)
        return self.job_proto.dataset_feature_statistics_list
