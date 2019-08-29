from concurrent import futures
import time
import logging

import grpc

import feast.serving.ServingService_pb2_grpc as Serving
from feast.serving.ServingService_pb2 import GetFeastServingVersionResponse

_ONE_DAY_IN_SECONDS = 60 * 60 * 24


class ServingServicer(Serving.ServingServiceServicer):
    def GetFeastServingVersion(self, request, context):
        return GetFeastServingVersionResponse(version="0.3.0")


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
