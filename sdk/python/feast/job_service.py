from concurrent.futures import ThreadPoolExecutor

import grpc

import feast
from feast.core import JobService_pb2_grpc
from feast.third_party.grpc.health.v1 import HealthService_pb2_grpc
from feast.third_party.grpc.health.v1.HealthService_pb2 import (
    HealthCheckResponse,
    ServingStatus,
)


class JobServiceServicer(JobService_pb2_grpc.JobServiceServicer):
    def __init__(self):
        self.client = feast.Client()

    def StartOfflineToOnlineIngestionJob(self, request, context):
        """Start job to ingest data from offline store into online store"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def GetHistoricalFeatures(self, request, context):
        """Produce a training dataset, return a job id that will provide a file reference"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def StartStreamToOnlineIngestionJob(self, request, context):
        """Start job to ingest data from stream into online store"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def ListJobs(self, request, context):
        """List all types of jobs"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def StopJob(self, request, context):
        """Stop a single job"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")

    def GetJob(self, request, context):
        """Get details of a single job"""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


class HealthServicer(HealthService_pb2_grpc.HealthServicer):
    def Check(self, request, context):
        return HealthCheckResponse(status=ServingStatus.SERVING)


def start_job_service():
    """
    Start Feast Job Service
    """
    server = grpc.server(ThreadPoolExecutor())
    JobService_pb2_grpc.add_JobServiceServicer_to_server(JobServiceServicer(), server)
    HealthService_pb2_grpc.add_HealthServicer_to_server(HealthServicer(), server)
    server.add_insecure_port("[::]:6568")
    server.start()
    print("Feast job server listening on port :6568")
    server.wait_for_termination()
