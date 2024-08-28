import asyncio
import logging

import grpc

from feast.errors import FeastObjectNotFoundException, to_error_detail
from feast.permissions.auth.auth_manager import (
    get_auth_manager,
)
from feast.permissions.security_manager import get_security_manager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class AuthInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        sm = get_security_manager()

        if sm is not None:
            auth_manager = get_auth_manager()
            access_token = auth_manager.token_extractor.extract_access_token(
                metadata=dict(handler_call_details.invocation_metadata)
            )

            print(f"Fetching user for token: {len(access_token)}")
            current_user = asyncio.run(
                auth_manager.token_parser.user_details_from_access_token(access_token)
            )
            print(f"User is: {current_user}")
            sm.set_current_user(current_user)

        return continuation(handler_call_details)


class ErrorInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        def exception_wrapper(behavior, request, context):
            try:
                return behavior(request, context)
            except grpc.RpcError as e:
                context.abort(e.code(), e.details())
            except Exception as e:
                context.abort(
                    _error_to_status_code(e),
                    to_error_detail(e),
                )

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


def _error_to_status_code(error: Exception) -> grpc.StatusCode:
    if isinstance(error, FeastObjectNotFoundException):
        return grpc.StatusCode.NOT_FOUND
    if isinstance(error, FeastObjectNotFoundException):
        return grpc.StatusCode.PERMISSION_DENIED
    return grpc.StatusCode.INTERNAL
