import logging
import time
from concurrent import futures

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from feast.core import CoreService_pb2_grpc as Core
from feast.core.CoreService_pb2 import (
    ApplyEntityRequest,
    ApplyEntityResponse,
    ApplyFeatureTableRequest,
    ApplyFeatureTableResponse,
    DeleteFeatureTableRequest,
    DeleteFeatureTableResponse,
    GetEntityRequest,
    GetEntityResponse,
    GetFeastCoreVersionResponse,
    GetFeatureTableRequest,
    GetFeatureTableResponse,
    ListEntitiesRequest,
    ListEntitiesResponse,
    ListFeatureTablesRequest,
    ListFeatureTablesResponse,
    ListProjectsResponse,
)
from feast.core.Entity_pb2 import Entity as EntityProto
from feast.core.Entity_pb2 import EntityMeta
from feast.core.FeatureTable_pb2 import FeatureTable as FeatureTableProto
from feast.core.FeatureTable_pb2 import FeatureTableMeta

_logger = logging.getLogger(__name__)

_ONE_DAY_IN_SECONDS = 60 * 60 * 24
_SIGNATURE_HEADER_KEY = "authorization"


class DisallowAuthInterceptor(grpc.ServerInterceptor):
    def __init__(self):
        def abort(ignored_request, context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid signature")

        self._abortion = grpc.unary_unary_rpc_method_handler(abort)

    def intercept_service(self, continuation, handler_call_details):
        print(handler_call_details.invocation_metadata)
        if "Bearer" in handler_call_details.invocation_metadata[0][1]:
            return self._abortion
        else:
            return continuation(handler_call_details)


class AllowAuthInterceptor(grpc.ServerInterceptor):
    def __init__(self):
        def abort(ignored_request, context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Invalid signature")

        self._abortion = grpc.unary_unary_rpc_method_handler(abort)

    def intercept_service(self, continuation, handler_call_details):
        print(handler_call_details.invocation_metadata)
        if "Bearer" in handler_call_details.invocation_metadata[0][1]:
            return continuation(handler_call_details)
        else:
            return self._abortion


class CoreServicer(Core.CoreServiceServicer):
    def __init__(self):
        self._feature_tables = dict()
        self._entities = dict()
        self._projects = ["default"]

    def GetFeastCoreVersion(self, request, context):
        return GetFeastCoreVersionResponse(version="0.10.0")

    def GetFeatureTable(self, request: GetFeatureTableRequest, context):
        filtered_table = [
            table
            for table in self._feature_tables.values()
            if table.spec.name == request.name
        ]
        return GetFeatureTableResponse(table=filtered_table[0])

    def ListFeatureTables(self, request: ListFeatureTablesRequest, context):

        filtered_feature_table_response = list(self._feature_tables.values())

        return ListFeatureTablesResponse(tables=filtered_feature_table_response)

    def ApplyFeatureTable(self, request: ApplyFeatureTableRequest, context):
        feature_table_spec = request.table_spec

        feature_table_meta = FeatureTableMeta(created_timestamp=Timestamp(seconds=10),)
        applied_feature_table = FeatureTableProto(
            spec=feature_table_spec, meta=feature_table_meta
        )
        self._feature_tables[feature_table_spec.name] = applied_feature_table

        _logger.info(
            "registered feature table "
            + feature_table_spec.name
            + " with "
            + str(len(feature_table_spec.entities))
            + " entities and "
            + str(len(feature_table_spec.features))
            + " features"
        )

        return ApplyFeatureTableResponse(table=applied_feature_table,)

    def DeleteFeatureTable(self, request: DeleteFeatureTableRequest, context):
        del self._feature_tables[request.name]
        return DeleteFeatureTableResponse()

    def GetEntity(self, request: GetEntityRequest, context):
        filtered_entities = [
            entity
            for entity in self._entities.values()
            if entity.spec.name == request.name
        ]
        return GetEntityResponse(entity=filtered_entities[0])

    def ListEntities(self, request: ListEntitiesRequest, context):

        filtered_entities_response = list(self._entities.values())

        return ListEntitiesResponse(entities=filtered_entities_response)

    def ListProjects(self, request, context):
        return ListProjectsResponse(projects=self._projects)

    def ApplyEntity(self, request: ApplyEntityRequest, context):
        entity_spec = request.spec

        entity_meta = EntityMeta(created_timestamp=Timestamp(seconds=10),)
        applied_entity = EntityProto(spec=entity_spec, meta=entity_meta)
        self._entities[entity_spec.name] = applied_entity

        _logger.info(
            "registered entity "
            + entity_spec.name
            + " with "
            + str(entity_spec.value_type)
            + " value"
        )

        return ApplyEntityResponse(entity=applied_entity,)


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
