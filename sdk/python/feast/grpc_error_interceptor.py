import grpc

from feast.errors import FeastError


def exception_wrapper(behavior, request, context):
    try:
        return behavior(request, context)
    except grpc.RpcError as e:
        context.abort(e.code(), e.details())
    except FeastError as e:
        context.abort(
            e.grpc_status_code(),
            e.to_error_detail(),
        )


class ErrorInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None

        if handler.unary_unary:
            return grpc.unary_unary_rpc_method_handler(
                lambda req, ctx: exception_wrapper(handler.unary_unary, req, ctx),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        elif handler.unary_stream:
            return grpc.unary_stream_rpc_method_handler(
                lambda req, ctx: exception_wrapper(handler.unary_stream, req, ctx),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        elif handler.stream_unary:
            return grpc.stream_unary_rpc_method_handler(
                lambda req, ctx: exception_wrapper(handler.stream_unary, req, ctx),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        elif handler.stream_stream:
            return grpc.stream_stream_rpc_method_handler(
                lambda req, ctx: exception_wrapper(handler.stream_stream, req, ctx),
                request_deserializer=handler.request_deserializer,
                response_serializer=handler.response_serializer,
            )
        return handler
