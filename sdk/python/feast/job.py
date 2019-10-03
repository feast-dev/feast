import tempfile
import time
from datetime import datetime, timedelta

import pandas as pd
from fastavro import reader as fastavro_reader
from google.cloud import storage

from feast.serving.ServingService_pb2 import (
    Job as JobProto,
    JOB_STATUS_DONE,
    DATA_FORMAT_AVRO,
)
from feast.serving.ServingService_pb2 import GetJobRequest
from feast.serving.ServingService_pb2_grpc import ServingServiceStub

# TODO: Need to profile and check the performance and memory consumption of
#       the current approach to read files into pandas DataFrame or iterate the
#       data row by row.

# Maximum no of seconds to wait until the jobs status is DONE in Feast
DEFAULT_TIMEOUT_SEC: int = 86400

# Maximum no of seconds to wait before reloading the job status in Feast
MAX_WAIT_INTERVAL_SEC: int = 60


class Job:
    """
    A class representing a job for feature retrieval in Feast.
    """

    # noinspection PyShadowingNames
    def __init__(
        self,
        job_proto: JobProto,
        serving_stub: ServingServiceStub,
        storage_client: storage.Client,
    ):
        """
        Args:
            job_proto: Job proto object (wrapped by this job object)
            serving_stub: Stub for Feast serving service
            storage_client: Google Cloud Storage client
        """
        self.job_proto = job_proto
        self.serving_stub = serving_stub
        self.storage_client = storage_client

    @property
    def id(self):
        return self.job_proto.id

    @property
    def status(self):
        return self.job_proto.status

    def reload(self):
        """
        Reload the latest job status
        Returns: None
        """
        self.job_proto = self.serving_stub.GetJob(GetJobRequest(job=self.job_proto)).job

    def result(self, timeout_sec: int = DEFAULT_TIMEOUT_SEC):
        """
        Wait until job is done to get an iterable rows of result
        The row represents can only represent an Avro row in Feast 0.3.

        Args:
            timeout_sec: max no of seconds to wait until job is done. If "timeout_sec" is exceeded, an exception will be raised.

        Returns: Iterable of Avro rows

        """
        max_wait_datetime = datetime.now() + timedelta(seconds=timeout_sec)
        wait_duration_sec = 2

        while self.status != JOB_STATUS_DONE:
            if datetime.now() > max_wait_datetime:
                raise Exception(
                    "Timeout exceeded while waiting for result. Please retry this method or use a longer timeout value."
                )

            self.reload()
            time.sleep(wait_duration_sec)
            # Backoff the wait duration exponentially up till MAX_WAIT_INTERVAL_SEC
            wait_duration_sec = min(wait_duration_sec * 2, MAX_WAIT_INTERVAL_SEC)

        if self.job_proto.error:
            raise Exception(self.job_proto.error)

        if self.job_proto.data_format != DATA_FORMAT_AVRO:
            raise Exception(
                "Feast only supports Avro data format for now. Please check "
                "your Feast Serving deployment."
            )

        for file_uri in self.job_proto.file_uris:
            if not file_uri.startswith("gs://"):
                raise Exception(
                    "Feast only supports reading from Google Cloud "
                    "Storage for now. Please check your Feast Serving deployment."
                )
            with tempfile.TemporaryFile() as file_obj:
                self.storage_client.download_blob_to_file(file_uri, file_obj)
                file_obj.seek(0)
                avro_reader = fastavro_reader(file_obj)
                for record in avro_reader:
                    yield record

    def to_dataframe(self, timeout_sec: int = DEFAULT_TIMEOUT_SEC):
        """
        Wait until job is done to get an interable rows of result
        Args:
            timeout_sec: max no of seconds to wait until job is done. If "timeout_sec" is exceeded, an exception will be raised.
        Returns: pandas Dataframe of the feature values
        """
        records = [r for r in self.result()]
        return pd.DataFrame.from_records(records)

    def __iter__(self):
        return iter(self.result())
