from typing import List

import grpc
from google.protobuf.json_format import MessageToJson

from feast import Source
from feast.core.CoreService_pb2 import ListIngestionJobsRequest
from feast.core.CoreService_pb2_grpc import JobControllerServiceStub
from feast.core.IngestionJob_pb2 import IngestionJob as IngestJobProto
from feast.core.IngestionJob_pb2 import IngestionJobStatus
from feast.core.Store_pb2 import Store
from feast.feature_set import FeatureSetRef
from feast.wait import wait_retry_backoff


class IngestJob:
    """
    Defines a job for feature ingestion in feast.
    """

    def __init__(
        self,
        job_proto: IngestJobProto,
        core_stub: JobControllerServiceStub,
        auth_metadata_plugin: grpc.AuthMetadataPlugin = None,
    ):
        """
        Construct a native ingest job from its protobuf version.

        Args:
        job_proto: Job proto object to construct from.
        core_stub: stub for Feast CoreService
        auth_metadata_plugin: plugin to fetch auth metadata
        """
        self.proto = job_proto
        self.core_svc = core_stub
        self.auth_metadata = auth_metadata_plugin

    def reload(self):
        """
        Update this IngestJob with the latest info from Feast
        """
        # pull latest proto from feast core
        response = self.core_svc.ListIngestionJobs(
            ListIngestionJobsRequest(
                filter=ListIngestionJobsRequest.Filter(id=self.id)
            ),
            metadata=self.auth_metadata.get_signed_meta() if self.auth_metadata else (),
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
    def status(self) -> IngestionJobStatus:  # type: ignore
        """
        Getter for IngestJob's status
        """
        self.reload()
        return self.proto.status

    @property
    def feature_sets(self) -> List[FeatureSetRef]:
        """
        Getter for the IngestJob's feature sets
        """
        # convert featureset protos to native objects
        return [
            FeatureSetRef.from_proto(fs) for fs in self.proto.feature_set_references
        ]

    @property
    def source(self) -> Source:
        """
        Getter for the IngestJob's data source.
        """
        return Source.from_proto(self.proto.source)

    @property
    def stores(self) -> List[Store]:
        """
        Getter for the IngestJob's target feast store.
        """
        return list(self.proto.stores)

    def wait(self, status: IngestionJobStatus, timeout_secs: int = 300):  # type: ignore
        """
        Wait for this IngestJob to transtion to the given status.
        Raises TimeoutError if the wait operation times out.

        Args:
            status: The IngestionJobStatus to wait for.
            timeout_secs: Maximum seconds to wait before timing out.
        """
        # poll & wait for job status to transition
        wait_retry_backoff(
            retry_fn=(lambda: (None, self.status == status)),  # type: ignore
            timeout_secs=timeout_secs,
            timeout_msg="Wait for IngestJob's status to transition timed out",
        )

    def __str__(self):
        # render the contents of ingest job as human readable string
        self.reload()
        return str(MessageToJson(self.proto))

    def __repr__(self):
        # render the ingest job as human readable string
        return f"IngestJob<{self.id}>"
