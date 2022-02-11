from concurrent import futures
import sys
import time

import grpc

from grpc_health.v1.health import HealthServicer
from grpc_health.v1 import health_pb2, health_pb2_grpc

from connector_python import Connector_pb2_grpc
from connector_python import Connector_pb2

class ConnectorOnlineStore(Connector_pb2_grpc.OnlineStoreServicer):
    """Implementation of KV service."""

    def OnlineRead(self, request, context):
        response = Connector_pb2.OnlineReadResponse()
        response.results = [[]]
        return response


def serve():
    # We need to build a health service to work with go-plugin
    health = HealthServicer()
    health.set("plugin", health_pb2.HealthCheckResponse.ServingStatus.Value('SERVING'))

    # Start the server.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    Connector_pb2_grpc.add_OnlineStoreServicer_to_server(ConnectorOnlineStore(), server)
    health_pb2_grpc.add_HealthServicer_to_server(health, server)
    server.add_insecure_port('127.0.0.1:1234')
    server.start()

    # Output information
    print("1|1|tcp|127.0.0.1:1234|grpc")
    sys.stdout.flush()

    try:
        while True:
            time.sleep(60 * 60 * 24)
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == '__main__':
    serve()
