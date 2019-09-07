from concurrent import futures
import time
import logging

import grpc

import feast.core.CoreService_pb2_grpc as Core
from feast.core.CoreService_pb2 import (
    GetFeastCoreVersionResponse,
    ApplyFeatureSetResponse,
    ApplyFeatureSetRequest,
    GetFeatureSetsResponse,
)
from feast.core.FeatureSet_pb2 import FeatureSetSpec as FeatureSetSpec
from typing import List

from google.protobuf import empty_pb2 as empty

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class CoreServicer(Core.CoreServiceServicer):
    def __init__(self):
        self._feature_sets = dict()

    def GetFeastCoreVersion(self, request, context):
        return GetFeastCoreVersionResponse(version="0.3.0")

    def GetFeatureSets(self, request: empty, context):
        feature_set_list = list(
            self._feature_sets.values()
        )  # type: List[FeatureSetSpec]
        return GetFeatureSetsResponse(featureSets=feature_set_list)

    def ApplyFeatureSet(self, request: ApplyFeatureSetRequest, context):
        feature_set = request.featureSet
        if feature_set.version is None:
            feature_set.version = 1
        else:
            feature_set.version = feature_set.version + 1
        self._feature_sets[feature_set.name] = feature_set
        return ApplyFeatureSetResponse(featureSet=feature_set)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Core.add_CoreServiceServicer_to_server(CoreServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    try:
        while True:
            time.sleep(_ONE_DAY_IN_SECONDS)
    except KeyboardInterrupt:
        server.stop(0)


if __name__ == "__main__":
    logging.basicConfig()
    serve()
