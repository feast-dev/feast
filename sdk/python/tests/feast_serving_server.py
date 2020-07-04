import logging
import time
from concurrent import futures
from typing import Dict

import grpc

from feast.core import FeatureSet_pb2 as FeatureSetProto
from feast.core.CoreService_pb2 import ListFeatureSetsResponse
from feast.core.CoreService_pb2_grpc import CoreServiceStub
from feast.serving import ServingService_pb2_grpc as Serving
from feast.serving.ServingService_pb2 import GetFeastServingInfoResponse

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class ServingServicer(Serving.ServingServiceServicer):
    def __init__(self, core_url: str = None):
        if core_url:
            self.__core_channel = None
            self.__connect_core(core_url)
            self._feature_sets = (
                dict()
            )  # type: Dict[str, FeatureSetProto.FeatureSetSpec]

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

    def GetFeastServingVersion(self, request, context):
        return GetFeastServingInfoResponse(version="0.3.2")


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
