import os
import subprocess
from concurrent import futures
from contextlib import contextmanager
from typing import List
from unittest.mock import patch

import grpc
import pyspark

from feast.client import Client
from feast.core.CoreService_pb2_grpc import add_CoreServiceServicer_to_server
from feast.data_format import ParquetFormat, ProtoFormat
from feast.data_source import FileSource, KafkaSource
from feast.entity import Entity
from feast.feature import Feature
from feast.feature_table import FeatureTable
from feast.job_service import ensure_stream_ingestion_jobs
from feast.pyspark.launchers.standalone import (
    StandaloneClusterLauncher,
    reset_job_cache,
)
from feast.value_type import ValueType
from tests.feast_core_server import CoreServicer as MockCoreServicer


@contextmanager
def mock_server(servicer, add_fn):
    """Instantiate a server and return its address for use in tests"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_fn(servicer, server)
    port = server.add_insecure_port("[::]:0")
    server.start()

    try:
        address = "localhost:%d" % port
        with grpc.insecure_channel(address):
            yield address
    finally:
        server.stop(None)


SERVING_URL = "serving.example.com"


class TestStreamingControlLoop:
    table_name = "my-feature-table-1"

    features_1 = [
        Feature(name="fs1-my-feature-1", dtype=ValueType.INT64),
        Feature(name="fs1-my-feature-2", dtype=ValueType.STRING),
        Feature(name="fs1-my-feature-3", dtype=ValueType.STRING_LIST),
        Feature(name="fs1-my-feature-4", dtype=ValueType.BYTES_LIST),
    ]

    features_2 = features_1 + [
        Feature(name="fs1-my-feature-5", dtype=ValueType.BYTES_LIST),
    ]

    def _create_ft(self, client: Client, features) -> None:
        entity = Entity(
            name="driver_car_id",
            description="Car driver id",
            value_type=ValueType.STRING,
            labels={"team": "matchmaking"},
        )

        # Register Entity with Core
        client.apply(entity)

        # Create Feature Tables
        batch_source = FileSource(
            file_format=ParquetFormat(),
            file_url="file://feast/*",
            event_timestamp_column="ts_col",
            created_timestamp_column="timestamp",
            date_partition_column="date_partition_col",
        )

        stream_source = KafkaSource(
            bootstrap_servers="localhost:9094",
            message_format=ProtoFormat("class.path"),
            topic="test_topic",
            event_timestamp_column="ts_col",
            created_timestamp_column="timestamp",
        )

        ft1 = FeatureTable(
            name=self.table_name,
            features=features,
            entities=["driver_car_id"],
            labels={"team": "matchmaking"},
            batch_source=batch_source,
            stream_source=stream_source,
        )

        # Register Feature Table with Core
        client.apply(ft1)

    def _delete_ft(self, client: Client):
        client.delete_feature_table(self.table_name)

    def test_streaming_job_control_loop(self) -> None:
        """ Test streaming job control loop logic. """

        reset_job_cache()

        core_servicer = MockCoreServicer()

        processes: List[subprocess.Popen] = []

        def _mock_spark_submit(self, *args, **kwargs) -> subprocess.Popen:
            # We mock StandaloneClusterLauncher.spark_submit to run a dummy process and pretend
            # that this is a spark structured streaming process. In addition, this implementation
            # will keep track of launched processes in an array.
            result = subprocess.Popen(args=["/bin/bash", "-c", "sleep 600"])
            processes.append(result)
            return result

        with patch.object(
            StandaloneClusterLauncher, "spark_submit", new=_mock_spark_submit
        ), mock_server(
            core_servicer, add_CoreServiceServicer_to_server
        ) as core_service_url:
            client = Client(
                core_url=core_service_url,
                serving_url=SERVING_URL,
                spark_launcher="standalone",
                spark_home=os.path.dirname(pyspark.__file__),
            )

            # Run one iteration of the control loop. It should do nothing since we have no
            # feature tables.
            ensure_stream_ingestion_jobs(client=client, all_projects=True)

            # No jobs should be running at this point.
            assert len(client.list_jobs(include_terminated=True)) == 0

            # Now, create a new feature table.
            self._create_ft(client, self.features_1)

            # Run another iteration of the control loop.
            ensure_stream_ingestion_jobs(client=client, all_projects=True)

            # We expect a streaming job to be created for the new Feature Table.
            assert len(client.list_jobs(include_terminated=False)) == 1
            assert len(processes) == 1

            first_job_id = client.list_jobs(include_terminated=False)[0].get_id()

            # Pretend that the streaming job has terminated for no reason.
            processes[0].kill()

            # The control loop is expected to notice the killed job and start it again.
            ensure_stream_ingestion_jobs(client=client, all_projects=True)

            # We expect to find one terminated job and one restarted job.
            assert len(client.list_jobs(include_terminated=False)) == 1
            assert len(client.list_jobs(include_terminated=True)) == 2

            id_after_restart = client.list_jobs(include_terminated=False)[0].get_id()

            # Indeed it is a new job with a new id.
            assert id_after_restart != first_job_id

            # Update the feature table.
            self._create_ft(client, self.features_2)

            # Run another iteration of the job control loop. We expect to restart the streaming
            # job since the feature table has changed.
            ensure_stream_ingestion_jobs(client=client, all_projects=True)

            # We expect to find two terminated job and one live job.
            assert len(client.list_jobs(include_terminated=False)) == 1
            assert len(client.list_jobs(include_terminated=True)) == 3

            id_after_change = client.list_jobs(include_terminated=False)[0].get_id()
            assert id_after_restart != id_after_change

            # Delete the feature table.
            self._delete_ft(client)

            # Run another iteration of the job control loop. We expect it to terminate the streaming
            # job.
            ensure_stream_ingestion_jobs(client=client, all_projects=True)

            assert len(client.list_jobs(include_terminated=False)) == 0
            assert len(client.list_jobs(include_terminated=True)) == 3
