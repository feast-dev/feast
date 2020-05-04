import logging
import time
from concurrent import futures

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import feast.core.CoreService_pb2_grpc as Core
from feast.core.CoreService_pb2 import (
    ApplyFeatureSetRequest,
    ApplyFeatureSetResponse,
    GetFeastCoreVersionResponse,
    ListFeatureSetsRequest,
    ListFeatureSetsResponse,
)
from feast.core.FeatureSet_pb2 import FeatureSet as FeatureSetProto
from feast.core.FeatureSet_pb2 import FeatureSetMeta, FeatureSetStatus
from feast.core.Source_pb2 import KafkaSourceConfig as KafkaSourceConfigProto
from feast.core.Source_pb2 import SourceType as SourceTypeProto

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
                or request.filter.feature_set_name == "*"
                or fs.spec.name == request.filter.feature_set_name
            )
        ]

        return ListFeatureSetsResponse(feature_sets=filtered_feature_set_response)

    def ApplyFeatureSet(self, request: ApplyFeatureSetRequest, context):
        feature_set = request.feature_set

        if feature_set.spec.source.type == SourceTypeProto.INVALID:
            feature_set.spec.source.kafka_source_config.CopyFrom(
                KafkaSourceConfigProto(bootstrap_servers="server.com", topic="topic1")
            )
            feature_set.spec.source.type = SourceTypeProto.KAFKA

        feature_set_meta = FeatureSetMeta(
            status=FeatureSetStatus.STATUS_READY,
            created_timestamp=Timestamp(seconds=10),
        )
        applied_feature_set = FeatureSetProto(
            spec=feature_set.spec, meta=feature_set_meta
        )
        self._feature_sets[feature_set.spec.name] = applied_feature_set

        _logger.info(
            "registered feature set "
            + feature_set.spec.name
            + " with "
            + str(len(feature_set.spec.entities))
            + " entities and "
            + str(len(feature_set.spec.features))
            + " features"
        )

        return ApplyFeatureSetResponse(
            feature_set=applied_feature_set,
            status=ApplyFeatureSetResponse.Status.CREATED,
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
