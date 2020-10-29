from concurrent.futures import ThreadPoolExecutor

import grpc

import feast
from feast.constants import CONFIG_SPARK_HISTORICAL_FEATURE_OUTPUT_FORMAT
from feast.core import JobService_pb2_grpc
from feast.core.JobService_pb2 import (
    GetHistoricalFeaturesResponse,
    StartOfflineToOnlineIngestionJobResponse,
    StartStreamToOnlineIngestionJobResponse,
)
from feast.data_source import DataSource
from feast.pyspark.launcher import (
    start_historical_feature_retrieval_job,
    start_offline_to_online_ingestion,
    start_stream_to_online_ingestion,
)
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
        feature_table = self.client.get_feature_table(
            request.table_name, request.project
        )
        job = start_offline_to_online_ingestion(
            feature_table,
            request.start_date.ToDatetime(),
            request.end_date.ToDatetime(),
            self.client,
        )
        return StartOfflineToOnlineIngestionJobResponse(id=job.get_id())

    def GetHistoricalFeatures(self, request, context):
        """Produce a training dataset, return a job id that will provide a file reference"""
        feature_tables = self.client._get_feature_tables_from_feature_refs(
            request.feature_refs, request.project
        )
        output_format = self.client._config.get(
            CONFIG_SPARK_HISTORICAL_FEATURE_OUTPUT_FORMAT
        )

        job = start_historical_feature_retrieval_job(
            self.client,
            DataSource.from_proto(request.entities_source),
            feature_tables,
            output_format,
            request.destination_path,
        )
        return GetHistoricalFeaturesResponse(id=job.get_id())

    def StartStreamToOnlineIngestionJob(self, request, context):
        """Start job to ingest data from stream into online store"""
        feature_table = self.client.get_feature_table(
            request.table_name, request.project
        )
        job = start_stream_to_online_ingestion(feature_table, [], self.client)
        return StartStreamToOnlineIngestionJobResponse(id=job.get_id())

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
