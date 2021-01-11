from collections import defaultdict
from concurrent import futures
from contextlib import contextmanager
from datetime import datetime

import grpc

from feast.core.JobService_pb2 import GetJobResponse
from feast.core.JobService_pb2 import Job as JobProto
from feast.core.JobService_pb2 import JobStatus, JobType
from feast.core.JobService_pb2_grpc import (
    JobServiceServicer,
    JobServiceStub,
    add_JobServiceServicer_to_server,
)
from feast.remote_job import RemoteRetrievalJob


@contextmanager
def mock_server(servicer):
    """Instantiate a helloworld server and return a stub for use in tests"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_JobServiceServicer_to_server(servicer, server)
    port = server.add_insecure_port("[::]:0")
    server.start()

    try:
        with grpc.insecure_channel("localhost:%d" % port) as channel:
            yield JobServiceStub(channel)
    finally:
        server.stop(None)


class TestRemoteJob:
    def test_remote_ingestion_job(self):
        """ Test wating for the remote ingestion job to complete """

        class MockServicer(JobServiceServicer):
            """
            The RemoteJob is expected to call GetJob until its done.
            This mock JobService returns RUNNING status on the first call, and DONE on the second.
            """

            _job_statuses = [JobStatus.JOB_STATUS_DONE, JobStatus.JOB_STATUS_RUNNING]
            _call_count = defaultdict(int)

            def GetJob(self, request, context):

                self._call_count["GetJob"] += 1
                return GetJobResponse(
                    job=JobProto(
                        id="test",
                        type=JobType.RETRIEVAL_JOB,
                        status=self._job_statuses.pop(),
                        retrieval=JobProto.RetrievalJobMeta(output_location="foo"),
                    )
                )

        mock_servicer = MockServicer()
        with mock_server(mock_servicer) as service:
            remote_job = RemoteRetrievalJob(
                service, lambda: {}, "test", "foo", datetime.now(), None
            )

            assert remote_job.get_output_file_uri(timeout_sec=2) == "foo"
            assert mock_servicer._call_count["GetJob"] == 2
