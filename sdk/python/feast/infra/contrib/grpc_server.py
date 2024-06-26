import logging
import threading
from concurrent import futures
from typing import Optional, Union

import grpc
import pandas as pd
from grpc_health.v1 import health, health_pb2_grpc

from feast.data_source import PushMode
from feast.errors import FeatureServiceNotFoundException, PushSourceNotFoundException
from feast.feature_service import FeatureService
from feast.feature_store import FeatureStore
from feast.protos.feast.serving.GrpcServer_pb2 import (
    PushResponse,
    WriteToOnlineStoreResponse,
)
from feast.protos.feast.serving.GrpcServer_pb2_grpc import (
    GrpcFeatureServerServicer,
    add_GrpcFeatureServerServicer_to_server,
)
from feast.protos.feast.serving.ServingService_pb2 import (
    GetOnlineFeaturesRequest,
    GetOnlineFeaturesResponse,
)

logger = logging.getLogger(__name__)


def parse(features):
    df = {}
    for i in features.keys():
        df[i] = [features.get(i)]
    return pd.DataFrame.from_dict(df)


class GrpcFeatureServer(GrpcFeatureServerServicer):
    fs: FeatureStore

    _shuting_down: bool = False
    _active_timer: Optional[threading.Timer] = None

    def __init__(self, fs: FeatureStore, registry_ttl_sec: int = 5):
        self.fs = fs
        self.registry_ttl_sec = registry_ttl_sec
        super().__init__()

        self._async_refresh()

    def Push(self, request, context):
        try:
            df = parse(request.features)
            if request.to == "offline":
                to = PushMode.OFFLINE
            elif request.to == "online":
                to = PushMode.ONLINE
            elif request.to == "online_and_offline":
                to = PushMode.ONLINE_AND_OFFLINE
            else:
                raise ValueError(
                    f"{request.to} is not a supported push format. Please specify one of these ['online', 'offline', "
                    f"'online_and_offline']."
                )
            self.fs.push(
                push_source_name=request.push_source_name,
                df=df,
                allow_registry_cache=request.allow_registry_cache,
                to=to,
            )
        except PushSourceNotFoundException as e:
            logger.exception(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return PushResponse(status=False)
        except Exception as e:
            logger.exception(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return PushResponse(status=False)
        return PushResponse(status=True)

    def WriteToOnlineStore(self, request, context):
        logger.warning(
            "write_to_online_store is deprecated. Please consider using Push instead"
        )
        try:
            df = parse(request.features)
            self.fs.write_to_online_store(
                feature_view_name=request.feature_view_name,
                df=df,
                allow_registry_cache=request.allow_registry_cache,
            )
        except Exception as e:
            logger.exception(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return PushResponse(status=False)
        return WriteToOnlineStoreResponse(status=True)

    def GetOnlineFeatures(self, request: GetOnlineFeaturesRequest, context):
        if request.HasField("feature_service"):
            logger.info(f"Requesting feature service: {request.feature_service}")
            try:
                features: Union[list[str], FeatureService] = (
                    self.fs.get_feature_service(
                        request.feature_service, allow_cache=True
                    )
                )
            except FeatureServiceNotFoundException as e:
                logger.error(f"Feature service {request.feature_service} not found")
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(str(e))
                return GetOnlineFeaturesResponse()
        else:
            features = list(request.features.val)

        result = self.fs.get_online_features(
            features,
            request.entities,
            request.full_feature_names,
        ).proto

        return result

    def _async_refresh(self):
        self.fs.refresh_registry()
        if self._shuting_down:
            return
        self._active_timer = threading.Timer(self.registry_ttl_sec, self._async_refresh)
        self._active_timer.start()


def get_grpc_server(
    address: str,
    fs: FeatureStore,
    max_workers: int,
    registry_ttl_sec: int,
):
    logger.info(f"Initializing gRPC server on {address}")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    add_GrpcFeatureServerServicer_to_server(
        GrpcFeatureServer(fs, registry_ttl_sec=registry_ttl_sec),
        server,
    )
    health_servicer = health.HealthServicer(
        experimental_non_blocking=True,
        experimental_thread_pool=futures.ThreadPoolExecutor(max_workers=max_workers),
    )
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    server.add_insecure_port(address)
    return server
