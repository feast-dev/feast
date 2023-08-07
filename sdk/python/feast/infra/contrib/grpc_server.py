from concurrent import futures

import grpc
import pandas as pd
import logging
from feast.data_source import PushMode
from feast.errors import PushSourceNotFoundException
from feast.feature_store import FeatureStore
from feast.protos.feast.serving.GrpcServer_pb2 import WriteToOnlineStoreResponse, PushResponse
from feast.protos.feast.serving.GrpcServer_pb2_grpc import (
    GrpcFeatureServerServicer,
    add_GrpcFeatureServerServicer_to_server,
)
from grpc_health.v1 import health
from grpc_health.v1 import health_pb2_grpc


def parse(features):
    df = {}
    for i in features.keys():
        df[i] = [features.get(i)]
    return pd.DataFrame.from_dict(df)


class GrpcFeatureServer(GrpcFeatureServerServicer):
    fs: FeatureStore

    def __init__(self, fs):
        self.fs = fs
        super().__init__()

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
            logging.exception(str(e))
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return PushResponse(status=False)
        except Exception as e:
            logging.exception(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return PushResponse(status=False)
        return PushResponse(status=True)

    def WriteToOnlineStore(self, request, context):
        logging.warning("write_to_online_store is deprecated. Please consider using Push instead")
        try:
            df = parse(request.features)
            self.fs.write_to_online_store(
                feature_view_name=request.feature_view_name,
                df=df,
                allow_registry_cache=request.allow_registry_cache,
            )
        except Exception as e:
            logging.exception(str(e))
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return PushResponse(status=False)
        return WriteToOnlineStoreResponse(status=True)


def get_grpc_server(address: str, fs: FeatureStore, max_workers: int):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    add_GrpcFeatureServerServicer_to_server(GrpcFeatureServer(fs), server)
    health_pb2_grpc.add_HealthServicer_to_server(health.HealthServicer(), server)
    server.add_insecure_port(address)
    return server
