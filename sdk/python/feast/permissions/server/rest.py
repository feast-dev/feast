"""
A module with utility functions to support authorizing the REST servers using the FastAPI framework.
"""

from typing import Any

from fastapi.requests import Request

from feast.permissions.auth.auth_manager import (
    get_auth_manager,
)
from feast.permissions.security_manager import get_security_manager


async def inject_user_details(request: Request) -> Any:
    """
    A function to extract the authorization token from a user request, extract the user details and propagate them to the
    current security manager, if any.
    """
    sm = get_security_manager()
    current_user = None
    if sm is not None:
        auth_manager = get_auth_manager()
        access_token = auth_manager.token_extractor.extract_access_token(
            request=request
        )
        current_user = await auth_manager.token_parser.user_details_from_access_token(
            access_token=access_token
        )

        sm.set_current_user(current_user)

    return current_user
