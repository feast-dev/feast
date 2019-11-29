from concurrent import futures
import time
import logging
import grpc
import feast.core.CoreService_pb2_grpc as Core
from feast.core.CoreService_pb2 import (
    GetFeastCoreVersionResponse,
    ApplyFeatureSetResponse,
    ApplyFeatureSetRequest,
    ListFeatureSetsResponse,
    ListFeatureSetsRequest,
)
from feast.core.FeatureSet_pb2 import FeatureSetSpec as FeatureSetSpec
from feast.core.Source_pb2 import (
    SourceType as SourceTypeProto,
    KafkaSourceConfig as KafkaSourceConfigProto,
)
from typing import List

_logger = logging.getLogger(__name__)

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class CoreServicer(Core.CoreServiceServicer):
    def __init__(self):
        self._feature_sets = dict()

    def GetFeastCoreVersion(self, request, context):
        return GetFeastCoreVersionResponse(version="0.3.2")

    def ListFeatureSets(self, request: ListFeatureSetsRequest, context):

        filtered_feature_set_response = [
            fs
            for fs in list(self._feature_sets.values())
            if (
                not request.filter.feature_set_name
                or fs.name == request.filter.feature_set_name
            )
            and (
                not request.filter.feature_set_version
                or str(fs.version) == request.filter.feature_set_version
            )
        ]

        return ListFeatureSetsResponse(feature_sets=filtered_feature_set_response)

    def ApplyFeatureSet(self, request: ApplyFeatureSetRequest, context):
        feature_set = request.feature_set

        if feature_set.version is None:
            feature_set.version = 1
        else:
            feature_set.version = feature_set.version + 1

        if feature_set.source.type == SourceTypeProto.INVALID:
            feature_set.source.kafka_source_config.CopyFrom(
                KafkaSourceConfigProto(bootstrap_servers="server.com", topic="topic1")
            )
            feature_set.source.type = SourceTypeProto.KAFKA

        self._feature_sets[feature_set.name] = feature_set

        _logger.info(
            "registered feature set "
            + feature_set.name
            + " with "
            + str(len(feature_set.entities))
            + " entities and "
            + str(len(feature_set.features))
            + " features"
        )

        return ApplyFeatureSetResponse(
            feature_set=feature_set, status=ApplyFeatureSetResponse.Status.CREATED
        )


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
