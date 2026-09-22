import asyncio
import logging
from typing import Any, Callable, Optional

import grpc
from starlette.authentication import AuthenticationError

from feast.permissions.auth.auth_manager import (
    get_auth_manager,
)
from feast.permissions.security_manager import get_security_manager

logger = logging.getLogger(__name__)

_REGISTRY_SERVICE = "/feast.registry.RegistryServer"

# Write RPCs that require a Bearer JWT when write_auth_only is enabled.
# Keep this as one allowlist so later tickets only shrink the unauthenticated set.
WRITE_RPC_METHODS = frozenset(
    f"{_REGISTRY_SERVICE}/{name}"
    for name in (
        "ApplyEntity",
        "DeleteEntity",
        "ApplyDataSource",
        "DeleteDataSource",
        "ApplyFeatureView",
        "DeleteFeatureView",
        "ApplyFeatureService",
        "DeleteFeatureService",
        "ApplySavedDataset",
        "DeleteSavedDataset",
        "ApplyValidationReference",
        "DeleteValidationReference",
        "ApplyPermission",
        "DeletePermission",
        "ApplyProject",
        "DeleteProject",
        "ApplyMaterialization",
        "UpdateInfra",
        "Commit",
    )
)


def is_write_rpc(method: Optional[str]) -> bool:
    return bool(method) and method in WRITE_RPC_METHODS


class AuthInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        sm = get_security_manager()

        if sm is not None:
            auth_manager = get_auth_manager()
            access_token = auth_manager.token_extractor.extract_access_token(
                metadata=dict(handler_call_details.invocation_metadata)
            )

            logger.debug(
                f"Fetching user details for token of length: {len(access_token)}"
            )
            current_user = asyncio.run(
                auth_manager.token_parser.user_details_from_access_token(access_token)
            )
            logger.debug(f"User is: {current_user}")
            sm.set_current_user(current_user)

        return continuation(handler_call_details)


class WritePathAuthInterceptor(grpc.ServerInterceptor):
    """
    Authenticate write RPCs only. JWT parse and set_current_user run on the
    ThreadPoolExecutor worker via handler wrap, not in intercept_service.

    Non-write methods (Get/List/Health/reflection/Refresh/Expedia search) pass
    through with no token and no RBAC.
    """

    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None

        if not is_write_rpc(handler_call_details.method):
            return handler

        metadata = dict(handler_call_details.invocation_metadata or ())
        return _wrap_write_handler(handler, metadata)


def _wrap_write_handler(handler, metadata: dict):
    if handler.unary_unary:
        return grpc.unary_unary_rpc_method_handler(
            lambda req, ctx: _authenticate_and_call(
                handler.unary_unary, req, ctx, metadata
            ),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
    if handler.unary_stream:
        return grpc.unary_stream_rpc_method_handler(
            lambda req, ctx: _authenticate_and_call(
                handler.unary_stream, req, ctx, metadata
            ),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
    if handler.stream_unary:
        return grpc.stream_unary_rpc_method_handler(
            lambda req, ctx: _authenticate_and_call(
                handler.stream_unary, req, ctx, metadata
            ),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
    if handler.stream_stream:
        return grpc.stream_stream_rpc_method_handler(
            lambda req, ctx: _authenticate_and_call(
                handler.stream_stream, req, ctx, metadata
            ),
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
    return handler


def _authenticate_and_call(
    behavior: Callable[..., Any], request, context, metadata: dict
):
    try:
        _set_user_from_metadata(metadata)
    except AuthenticationError as e:
        context.abort(grpc.StatusCode.UNAUTHENTICATED, str(e))
    return behavior(request, context)


def _set_user_from_metadata(metadata: dict) -> None:
    auth_manager = get_auth_manager()
    access_token = auth_manager.token_extractor.extract_access_token(metadata=metadata)
    logger.debug(f"Fetching user details for token of length: {len(access_token)}")

    parser = auth_manager.token_parser
    sync_parse = getattr(parser, "user_details_from_access_token_sync", None)
    if callable(sync_parse):
        current_user = sync_parse(access_token)
    else:
        current_user = asyncio.run(parser.user_details_from_access_token(access_token))

    logger.debug(f"User is: {current_user}")
    sm = get_security_manager()
    if sm is not None:
        sm.set_current_user(current_user)
