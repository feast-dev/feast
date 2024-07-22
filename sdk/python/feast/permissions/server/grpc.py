import asyncio
import logging
from typing import Optional

import grpc

from feast.permissions.auth.auth_manager import (
    get_auth_manager,
)
from feast.permissions.security_manager import get_security_manager
from feast.permissions.server.utils import (
    AuthManagerType,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def grpc_interceptors(
    auth_type: AuthManagerType,
) -> Optional[list[grpc.ServerInterceptor]]:
    """
    A list of the authorization interceptors.

    Args:
        auth_type: The type of authorization manager, from the feature store configuration.

    Returns:
        list[grpc.ServerInterceptor]: Optional list of interceptors. If the authorization type is set to `NONE`, it returns `None`.
    """
    if auth_type == AuthManagerType.NONE:
        return None

    return [AuthInterceptor()]


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
