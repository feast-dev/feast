from typing import Optional

import grpc

from feast.config import Config
from feast.constants import (
    CONFIG_CORE_ENABLE_SSL_KEY,
    CONFIG_CORE_SERVER_SSL_CERT_KEY,
    CONFIG_ENABLE_AUTH_KEY,
    CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY,
    CONFIG_JOB_CONTROLLER_SERVER_KEY,
)
from feast.contrib.job_controller.job import IngestJob
from feast.core.CoreService_pb2 import (
    ListIngestionJobsRequest,
    RestartIngestionJobRequest,
    StopIngestionJobRequest,
)
from feast.core.CoreService_pb2_grpc import JobControllerServiceStub
from feast.feature_set import FeatureSetRef
from feast.grpc import auth as feast_auth
from feast.grpc.grpc import create_grpc_channel


class Client:
    """
    JobController Client: used internally to manage Ingestion Jobs
    """

    def __init__(self, options=None, **kwargs):
        """
        JobControllerClient should be initialized with
            jobcontroller_url: Feast JobController address

        :param options: Configuration options to initialize client with
        :param kwargs: options in kwargs style
        """
        if options is None:
            options = dict()
        self._config = Config(options={**options, **kwargs})

        self._jobcontroller_service_stub: Optional[JobControllerServiceStub] = None
        self._auth_metadata: Optional[grpc.AuthMetadataPlugin] = None

        # Configure Auth Metadata Plugin if auth is enabled
        if self._config.getboolean(CONFIG_ENABLE_AUTH_KEY):
            self._auth_metadata = feast_auth.get_auth_metadata_plugin(self._config)

    @property
    def _jobcontroller_service(self):
        if not self._jobcontroller_service_stub:
            channel = create_grpc_channel(
                url=self._config.get(CONFIG_JOB_CONTROLLER_SERVER_KEY),
                enable_ssl=self._config.getboolean(CONFIG_CORE_ENABLE_SSL_KEY),
                enable_auth=self._config.getboolean(CONFIG_ENABLE_AUTH_KEY),
                ssl_server_cert_path=self._config.get(CONFIG_CORE_SERVER_SSL_CERT_KEY),
                auth_metadata_plugin=self._auth_metadata,
                timeout=self._config.getint(CONFIG_GRPC_CONNECTION_TIMEOUT_DEFAULT_KEY),
            )
            self._jobcontroller_service_stub = JobControllerServiceStub(channel)

        return self._jobcontroller_service_stub

    def list_ingest_jobs(
        self,
        job_id: str = None,
        feature_set_ref: FeatureSetRef = None,
        store_name: str = None,
    ):
        """
        List the ingestion jobs currently registered in Feast, with optional filters.
        Provides detailed metadata about each ingestion job.

        Args:
            job_id: Select specific ingestion job with the given job_id
            feature_set_ref: Filter ingestion jobs by target feature set (via reference)
            store_name: Filter ingestion jobs by target feast store's name

        Returns:
            List of IngestJobs matching the given filters
        """
        # construct list request
        feature_set_ref_proto = None
        if feature_set_ref:
            feature_set_ref_proto = feature_set_ref.to_proto()
        list_filter = ListIngestionJobsRequest.Filter(
            id=job_id,
            feature_set_reference=feature_set_ref_proto,
            store_name=store_name,
        )
        request = ListIngestionJobsRequest(filter=list_filter)
        # make list request & unpack response
        response = self._jobcontroller_service.ListIngestionJobs(request, metadata=self._get_grpc_metadata(),)  # type: ignore
        ingest_jobs = [
            IngestJob(proto, self._jobcontroller_service, auth_metadata_plugin=self._auth_metadata) for proto in response.jobs  # type: ignore
        ]

        return ingest_jobs

    def restart_ingest_job(self, job: IngestJob):
        """
        Restart ingestion job currently registered in Feast.
        NOTE: Data might be lost during the restart for some job runners.
        Does not support stopping a job in a transitional (ie pending, suspending, aborting),
        terminal state (ie suspended or aborted) or unknown status

        Args:
            job: IngestJob to restart
        """
        request = RestartIngestionJobRequest(id=job.id)
        try:
            self._jobcontroller_service.RestartIngestionJob(
                request, metadata=self._get_grpc_metadata(),
            )  # type: ignore
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())

    def stop_ingest_job(self, job: IngestJob):
        """
        Stop ingestion job currently resgistered in Feast
        Does nothing if the target job if already in a terminal state (ie suspended or aborted).
        Does not support stopping a job in a transitional (ie pending, suspending, aborting)
        or in a unknown status

        Args:
            job: IngestJob to restart
        """
        request = StopIngestionJobRequest(id=job.id)
        try:
            self._jobcontroller_service.StopIngestionJob(
                request, metadata=self._get_grpc_metadata(),
            )  # type: ignore
        except grpc.RpcError as e:
            raise grpc.RpcError(e.details())

    def _get_grpc_metadata(self):
        """
        Returns a metadata tuple to attach to gRPC requests. This is primarily
        used when authentication is enabled but SSL/TLS is disabled.

        Returns: Tuple of metadata to attach to each gRPC call
        """
        if self._config.getboolean(CONFIG_ENABLE_AUTH_KEY) and self._auth_metadata:
            return self._auth_metadata.get_signed_meta()
        return ()
