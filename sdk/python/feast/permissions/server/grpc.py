import asyncio
import logging

import grpc

from feast.permissions.auth.auth_manager import (
    get_auth_manager,
)
from feast.permissions.security_manager import get_security_manager

logger = logging.getLogger(__name__)


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
