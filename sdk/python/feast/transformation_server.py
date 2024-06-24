import collections
import logging
import sys
import threading
from concurrent import futures

import grpc
import pyarrow as pa
from grpc_reflection.v1alpha import reflection

from feast.errors import OnDemandFeatureViewNotFoundException
from feast.feature_store import FeatureStore
from feast.protos.feast.serving.TransformationService_pb2 import (
    DESCRIPTOR,
    TRANSFORMATION_SERVICE_TYPE_PYTHON,
    GetTransformationServiceInfoResponse,
    TransformFeaturesResponse,
    ValueType,
)
from feast.protos.feast.serving.TransformationService_pb2_grpc import (
    TransformationServiceServicer,
    add_TransformationServiceServicer_to_server,
)
# from feast.protos.feast.third_party.grpc.health.v1.HealthService_pb2 import (
#     HealthCheckResponse,
#     ServingStatus,
# )
# from feast.protos.feast.third_party.grpc.health.v1.HealthService_pb2_grpc import (
#     HealthServicer,
#     add_HealthServicer_to_server,
# )
from feast.version import get_version

log = logging.getLogger(__name__)


# class _Watcher:
#     def __init__(self):
#         self._condition = threading.Condition()
#         self._responses = collections.deque()
#         self._open = True
#
#     def __iter__(self):
#         return self
#
#     def _next(self):
#         with self._condition:
#             while not self._responses and self._open:
#                 self._condition.wait()
#             if self._responses:
#                 return self._responses.popleft()
#             else:
#                 raise StopIteration()
#
#     def next(self):
#         return self._next()
#
#     def __next__(self):
#         return self._next()
#
#     def add(self, response):
#         with self._condition:
#             self._responses.append(response)
#             self._condition.notify()
#
#     def close(self):
#         with self._condition:
#             self._open = False
#             self._condition.notify()
#
#
# def _watcher_to_send_response_callback_adapter(watcher):
#     def send_response_callback(response):
#         if response is None:
#             watcher.close()
#         else:
#             watcher.add(response)
#
#     return send_response_callback
#
#
# class HealthServer(HealthServicer):
#     """Servicer handling RPCs for service statuses."""
#
#     def __init__(self, experimental_non_blocking=True, experimental_thread_pool=None):
#         self._lock = threading.RLock()
#         self._server_status = {"": ServingStatus.SERVING}
#         self._send_response_callbacks = {}
#         self.Watch.__func__.experimental_non_blocking = experimental_non_blocking
#         self.Watch.__func__.experimental_thread_pool = experimental_thread_pool
#         self._gracefully_shutting_down = False
#
#     def _on_close_callback(self, send_response_callback, service):
#         def callback():
#             with self._lock:
#                 self._send_response_callbacks[service].remove(send_response_callback)
#             send_response_callback(None)
#
#         return callback
#
#     def Check(self, request, context):
#         with self._lock:
#             status = self._server_status.get(request.service)
#             if status is None:
#                 context.set_code(grpc.StatusCode.NOT_FOUND)
#                 return HealthCheckResponse()
#             else:
#                 return HealthCheckResponse(status=status)
#
#     # pylint: disable=arguments-differ
#     def Watch(self, request, context, send_response_callback=None):
#         blocking_watcher = None
#         if send_response_callback is None:
#             # The server does not support the experimental_non_blocking
#             # parameter. For backwards compatibility, return a blocking response
#             # generator.
#             blocking_watcher = _Watcher()
#             send_response_callback = _watcher_to_send_response_callback_adapter(
#                 blocking_watcher
#             )
#         service = request.service
#         with self._lock:
#             status = self._server_status.get(service)
#             if status is None:
#                 status = ServingStatus.SERVICE_UNKNOWN  # pylint: disable=no-member
#             send_response_callback(HealthCheckResponse(status=status))
#             if service not in self._send_response_callbacks:
#                 self._send_response_callbacks[service] = set()
#             self._send_response_callbacks[service].add(send_response_callback)
#             context.add_callback(
#                 self._on_close_callback(send_response_callback, service)
#             )
#         return blocking_watcher
#
#     def set(self, service, status):
#         """Sets the status of a service.
#
#         Args:
#           service: string, the name of the service.
#           status: HealthCheckResponse.status enum value indicating the status of
#             the service
#         """
#         with self._lock:
#             if self._gracefully_shutting_down:
#                 return
#             else:
#                 self._server_status[service] = status
#                 if service in self._send_response_callbacks:
#                     for send_response_callback in self._send_response_callbacks[
#                         service
#                     ]:
#                         send_response_callback(HealthCheckResponse(status=status))
#
#     def enter_graceful_shutdown(self):
#         """Permanently sets the status of all services to NOT_SERVING.
#
#         This should be invoked when the server is entering a graceful shutdown
#         period. After this method is invoked, future attempts to set the status
#         of a service will be ignored.
#
#         This is an EXPERIMENTAL API.
#         """
#         with self._lock:
#             if self._gracefully_shutting_down:
#                 return
#             else:
#                 for service in self._server_status:
#                     self.set(
#                         service, ServingStatus.NOT_SERVING
#                     )  # pylint: disable=no-member
#                 self._gracefully_shutting_down = True


class TransformationServer(TransformationServiceServicer):
    def __init__(self, fs: FeatureStore) -> None:
        super().__init__()
        self.fs = fs

    def GetTransformationServiceInfo(self, request, context):
        response = GetTransformationServiceInfoResponse(
            type=TRANSFORMATION_SERVICE_TYPE_PYTHON,
            transformation_service_type_details=f"Python: {sys.version}, Feast: {get_version()}",
        )
        return response

    def TransformFeatures(self, request, context):
        try:
            odfv = self.fs.get_on_demand_feature_view(
                name=request.on_demand_feature_view_name, allow_cache=True
            )
        except OnDemandFeatureViewNotFoundException:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            raise

        df = pa.ipc.open_file(request.transformation_input.arrow_value).read_pandas()

        result_df = odfv.get_transformed_features_df(df, True)
        result_arrow = pa.Table.from_pandas(result_df)
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_file(sink, result_arrow.schema)
        writer.write_table(result_arrow)
        writer.close()

        buf = sink.getvalue().to_pybytes()

        return TransformFeaturesResponse(
            transformation_output=ValueType(arrow_value=buf)
        )


def start_server(store: FeatureStore, port: int):
    log.info("Starting server..")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    add_TransformationServiceServicer_to_server(TransformationServer(store), server)

    # Add health check service to server
#     add_HealthServicer_to_server(HealthServer(), server)

    service_names_available_for_reflection = (
        DESCRIPTOR.services_by_name["TransformationService"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(service_names_available_for_reflection, server)
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    server.wait_for_termination()
