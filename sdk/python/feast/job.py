import tempfile
import time
from datetime import datetime, timedelta
from typing import List
from urllib.parse import urlparse

import fastavro
import pandas as pd
from google.cloud import storage
from google.protobuf.json_format import MessageToJson

from feast.core.CoreService_pb2 import ListIngestionJobsRequest
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.core.IngestionJob_pb2 import IngestionJob as IngestJobProto
from feast.core.IngestionJob_pb2 import IngestionJobStatus
from feast.core.Store_pb2 import Store
from feast.feature_set import FeatureSet
from feast.serving.ServingService_pb2 import (
    DATA_FORMAT_AVRO,
    JOB_STATUS_DONE,
    GetJobRequest,
)
from feast.serving.ServingService_pb2 import Job as JobProto
from feast.serving.ServingService_pb2_grpc import ServingServiceStub
from feast.source import Source

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
        self, job_proto: JobProto, serving_stub: ServingServiceStub,
    ):
        """
        Args:
            job_proto: Job proto object (wrapped by this job object)
            serving_stub: Stub for Feast serving service
        """
        self.job_proto = job_proto
        self.serving_stub = serving_stub
        # TODO: abstract away GCP depedency
        self.gcs_client = storage.Client(project=None)

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
        self.job_proto = self.serving_stub.GetJob(GetJobRequest(job=self.job_proto)).job

    def get_avro_files(self, timeout_sec: int = DEFAULT_TIMEOUT_SEC):
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
        max_wait_datetime = datetime.now() + timedelta(seconds=timeout_sec)
        wait_duration_sec = 2

        while self.status != JOB_STATUS_DONE:
            if datetime.now() > max_wait_datetime:
                raise Exception(
                    "Timeout exceeded while waiting for result. Please retry "
                    "this method or use a longer timeout value."
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

        return [urlparse(uri) for uri in self.job_proto.file_uris]

    def result(self, timeout_sec: int = DEFAULT_TIMEOUT_SEC):
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
            if file_uri.scheme == "gs":
                file_obj = tempfile.TemporaryFile()
                self.gcs_client.download_blob_to_file(file_uri.geturl(), file_obj)
            elif file_uri.scheme == "file":
                file_obj = open(file_uri.path, "rb")
            else:
                raise Exception(
                    f"Could not identify file URI {file_uri}. Only gs:// and file:// supported"
                )

            file_obj.seek(0)
            avro_reader = fastavro.reader(file_obj)

            for record in avro_reader:
                yield record

    def to_dataframe(self, timeout_sec: int = DEFAULT_TIMEOUT_SEC) -> pd.DataFrame:
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
        self, max_chunk_size: int = -1, timeout_sec: int = DEFAULT_TIMEOUT_SEC
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
        # Max chunk size defined by user
        records = []
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


class IngestJob:
    """
    Defines a job for feature ingestion in feast.
    """

    def __init__(self, job_proto: IngestJobProto, core_stub: CoreServiceStub):
        """
        Construct a native ingest job from its protobuf version.

        Args:
        job_proto: Job proto object to construct from.
        core_stub: stub for Feast CoreService
        """
        self.proto = job_proto
        self.core_svc = core_stub

    def reload(self):
        """
        Update this IngestJob with the latest info from Feast
        """
        # pull latest proto from feast core
        response = self.core_svc.ListIngestionJobs(
            ListIngestionJobsRequest(filter=ListIngestionJobsRequest.Filter(id=self.id))
        )
        self.proto = response.jobs[0]

    @property
    def id(self) -> str:
        """
        Getter for IngestJob's job id.
        """
        return self.proto.id

    @property
    def external_id(self) -> str:
        """
        Getter for IngestJob's external job id.
        """
        self.reload()
        return self.proto.external_id

    @property
    def status(self) -> IngestionJobStatus:
        """
        Getter for IngestJob's status
        """
        self.reload()
        return self.proto.status

    @property
    def feature_sets(self) -> List[FeatureSet]:
        """
        Getter for the IngestJob's feature sets
        """
        # convert featureset protos to native objects
        return [FeatureSet.from_proto(fs) for fs in self.proto.feature_sets]

    @property
    def source(self) -> Source:
        """
        Getter for the IngestJob's data source.
        """
        return Source.from_proto(self.proto.source)

    @property
    def store(self) -> Store:
        """
        Getter for the IngestJob's target feast store.
        """
        return self.proto.store

    def wait(self, status: IngestionJobStatus, timeout_secs: float = 300):
        """
        Wait for this IngestJob to transtion to the given status.
        Raises TimeoutError if the wait operation times out.

        Args:
            status: The IngestionJobStatus to wait for.
            timeout_secs: Maximum seconds to wait before timing out.
        """
        # poll & wait for job status to transition
        wait_begin = time.time()
        wait_secs = 2
        elapsed_secs = 0
        while self.status != status and elapsed_secs <= timeout_secs:
            time.sleep(wait_secs)
            # back off wait duration exponentially, capped at MAX_WAIT_INTERVAL_SEC
            wait_secs = min(wait_secs * 2, MAX_WAIT_INTERVAL_SEC)
            elapsed_secs = time.time() - wait_begin

        # raise error if timeout
        if elapsed_secs > timeout_secs:
            raise TimeoutError("Wait for IngestJob's status to transition timed out")

    def __str__(self):
        # render the contents of ingest job as human readable string
        self.reload()
        return str(MessageToJson(self.proto))

    def __repr__(self):
        # render the ingest job as human readable string
        return f"IngestJob<{self.id}>"
