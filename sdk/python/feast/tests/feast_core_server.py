from concurrent import futures
import time
import logging

import grpc

import feast.core.CoreService_pb2_grpc as Core
from feast.core.CoreService_pb2 import GetFeastCoreVersionResponse

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class CoreServicer(Core.CoreServiceServicer):
    def GetFeastCoreVersion(self, request, context):
        return GetFeastCoreVersionResponse(version="0.3.0")


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
