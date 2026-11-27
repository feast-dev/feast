import asyncio
import logging
from typing import Optional

import grpc

from feast.permissions.auth.auth_manager import (
    get_auth_manager,
)
from feast.permissions.security_manager import get_security_manager

logger = logging.getLogger(__name__)


def _request_project(request) -> Optional[str]:
    """Return the project name of a registry request, or None.

    Most requests carry the project name in the field ``project``. In
    ``ApplyProjectRequest``, this field holds the whole ``Project`` message, so
    the name is in its ``spec``.
    """
    project = getattr(request, "project", None)
    if project is None or isinstance(project, str):
        return project
    spec = getattr(project, "spec", None)
    name = getattr(spec, "name", None)
    return name if isinstance(name, str) else None


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

        handler = continuation(handler_call_details)
        if sm is None or handler is None or handler.unary_unary is None:
            return handler
        behavior = handler.unary_unary

        def project_aware_behavior(request, context):
            token = sm.set_current_project(_request_project(request))
            try:
                return behavior(request, context)
            finally:
                sm.reset_current_project(token)

        return grpc.unary_unary_rpc_method_handler(
            project_aware_behavior,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
