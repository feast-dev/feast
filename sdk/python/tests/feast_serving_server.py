from concurrent import futures
import time
import logging

import grpc
import threading
import feast.serving.ServingService_pb2_grpc as Serving
from feast.serving.ServingService_pb2 import (
    GetBatchFeaturesResponse,
    GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
    GetFeastServingInfoResponse,
)
import fake_kafka
from typing import Dict
import sqlite3
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.core.CoreService_pb2 import (
    ListFeatureSetsResponse,
    ListStoresRequest,
    ListStoresResponse,
)
from feast.core import FeatureSet_pb2 as FeatureSetProto
import stores
from feast.types import (
    FeatureRow_pb2 as FeatureRowProto,
    Field_pb2 as FieldProto,
    Value_pb2 as ValueProto,
)
from google.protobuf.timestamp_pb2 import Timestamp

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class ServingServicer(Serving.ServingServiceServicer):
    def __init__(self, kafka: fake_kafka = None, core_url: str = None):
        if kafka and core_url:
            self.__core_channel = None
            self.__connect_core(core_url)
            self._feature_sets = (
                dict()
            )  # type: Dict[str, FeatureSetProto.FeatureSetSpec]
            self._kafka = kafka
            self._store = stores.SQLiteDatabase()

            thread = threading.Thread(target=self.__consume, args=())
            thread.daemon = True
            thread.start()

    def __connect_core(self, core_url: str):
        if not core_url:
            raise ValueError("Please set Feast Core URL.")

        if self.__core_channel is None:
            self.__core_channel = grpc.insecure_channel(core_url)

        try:
            grpc.channel_ready_future(self.__core_channel).result(timeout=5)
        except grpc.FutureTimeoutError:
            raise ConnectionError(
                "connection timed out while attempting to connect to Feast Core gRPC server "
                + core_url
            )
        else:
            self._core_service_stub = CoreServiceStub(self.__core_channel)

    def __get_feature_sets_from_core(self):
        # Get updated list of feature sets
        feature_sets = (
            self._core_service_stub.ListFeatureSets
        )  # type: ListFeatureSetsResponse

        # Store each feature set locally
        for feature_set in list(feature_sets.feature_sets):
            self._feature_sets[feature_set.name] = feature_set

    def __consume(self):
        """
        Consume message in the background from Fake Kafka
        """
        while True:
            self.__get_feature_sets_from_core()
            self.__register_feature_sets_with_store()
            for feature_set in list(self._feature_sets.values()):
                message = self._kafka.get(feature_set.source.kafka_source_config.topic)
                if message is None:
                    break
                self._store.upsert_feature_row(feature_set, message)
            time.sleep(1)

    def __register_feature_sets_with_store(self):
        for feature_set in list(self._feature_sets.values()):
            self._store.register_feature_set(feature_set)

    def GetFeastServingVersion(self, request, context):
        return GetFeastServingInfoResponse(version="0.3.2")

    def GetOnlineFeatures(self, request: GetOnlineFeaturesRequest, context):

        response = GetOnlineFeaturesResponse(
            feature_data_sets=[
                GetOnlineFeaturesResponse.FeatureDataSet(
                    name="feature_set_1",
                    version="1",
                    feature_rows=[
                        FeatureRowProto.FeatureRow(
                            feature_set="feature_set_1",
                            event_timestamp=Timestamp(),
                            fields=[
                                FieldProto.Field(
                                    name="feature_1",
                                    value=ValueProto.Value(float_val=1.2),
                                ),
                                FieldProto.Field(
                                    name="feature_2",
                                    value=ValueProto.Value(float_val=1.2),
                                ),
                                FieldProto.Field(
                                    name="feature_3",
                                    value=ValueProto.Value(float_val=1.2),
                                ),
                            ],
                        ),
                        FeatureRowProto.FeatureRow(
                            feature_set="feature_set_1",
                            event_timestamp=Timestamp(),
                            fields=[
                                FieldProto.Field(
                                    name="feature_1",
                                    value=ValueProto.Value(float_val=1.2),
                                ),
                                FieldProto.Field(
                                    name="feature_2",
                                    value=ValueProto.Value(float_val=1.2),
                                ),
                                FieldProto.Field(
                                    name="feature_3",
                                    value=ValueProto.Value(float_val=1.2),
                                ),
                            ],
                        ),
                        FeatureRowProto.FeatureRow(
                            feature_set="feature_set_1",
                            event_timestamp=Timestamp(),
                            fields=[
                                FieldProto.Field(
                                    name="feature_1",
                                    value=ValueProto.Value(float_val=1.2),
                                ),
                                FieldProto.Field(
                                    name="feature_2",
                                    value=ValueProto.Value(float_val=1.2),
                                ),
                                FieldProto.Field(
                                    name="feature_3",
                                    value=ValueProto.Value(float_val=1.2),
                                ),
                            ],
                        ),
                    ],
                )
            ]
        )

        return response


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Serving.add_ServingServiceServicer_to_server(ServingServicer(), server)
    server.add_insecure_port("[::]:50052")
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    logging.basicConfig()
    serve()
