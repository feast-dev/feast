"""
A module with utility functions and classes to support authorizing the Arrow Flight servers.
"""

import asyncio
import functools
import logging
from typing import cast

import pyarrow.flight as fl
from pyarrow.flight import ServerCallContext

from feast.permissions.auth.auth_manager import (
    get_auth_manager,
)
from feast.permissions.security_manager import get_security_manager
from feast.permissions.user import User

logger = logging.getLogger(__name__)


class AuthorizationMiddlewareFactory(fl.ServerMiddlewareFactory):
    """
    A middleware factory to intercept the authorization header and propagate it to the authorization middleware.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def start_call(self, info, headers):
        """
        Intercept the authorization header and propagate it to the authorization middleware.
        """
        access_token = get_auth_manager().token_extractor.extract_access_token(
            headers=headers
        )
        return AuthorizationMiddleware(access_token=access_token)


class AuthorizationMiddleware(fl.ServerMiddleware):
    """
    A server middleware holding the authorization header and offering a method to extract the user credentials.
    """

    def __init__(self, access_token: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.access_token = access_token

    def call_completed(self, exception):
        if exception:
            logger.exception(
                f"{AuthorizationMiddleware.__name__} encountered an exception: {exception}"
            )

    async def extract_user(self) -> User:
        """
        Use the configured `TokenParser` to extract the user credentials.
        """
        return await get_auth_manager().token_parser.user_details_from_access_token(
            self.access_token
        )


def inject_user_details(context: ServerCallContext):
    """
    Function to use in Arrow Flight endpoints (e.g. `do_get`, `do_put` and so on) to access the token extracted from the header,
    extract the user details out of it and propagate them to the current security manager, if any.

    Args:
        context: The endpoint context.
    """
    if context.get_middleware("auth") is None:
        logger.warning("No `auth` middleware.")
        return

    sm = get_security_manager()
    if sm is not None:
        auth_middleware = cast(AuthorizationMiddleware, context.get_middleware("auth"))
        current_user = asyncio.run(auth_middleware.extract_user())
        logger.debug(f"User extracted: {current_user}")

        sm.set_current_user(current_user)


def inject_user_details_decorator(func):
    @functools.wraps(func)
    def wrapper(self, context, *args, **kwargs):
        inject_user_details(context)
        return func(self, context, *args, **kwargs)

    return wrapper
