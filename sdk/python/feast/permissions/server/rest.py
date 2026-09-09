"""
A module with utility functions to support authorizing the REST servers using the FastAPI framework.
"""

from typing import Any, AsyncIterator, Optional

from fastapi import HTTPException
from fastapi.requests import Request

from feast.permissions.auth.auth_manager import (
    get_auth_manager,
)
from feast.permissions.security_manager import get_security_manager


def _extract_project_from_request(request: Request) -> Optional[str]:
    """Best-effort project extraction for REST authorization scoping.

    Most registry REST endpoints pass ``project`` as a query parameter.
    When absent (e.g. POST body-only), ``grpc_call`` still scopes from the
    protobuf request's ``project`` field.
    """
    project = request.query_params.get("project")
    if project:
        return project
    return None


async def inject_user_details(request: Request) -> AsyncIterator[Any]:
    """
    Extract the authorization token from a user request, propagate user details
    to the security manager, and scope permissions to the request project when
    available (query param ``project``).

    Yields so project context is reset after the request completes.
    """
    sm = get_security_manager()
    current_user = None
    project_token = None
    if sm is not None:
        try:
            auth_manager = get_auth_manager()
            access_token = auth_manager.token_extractor.extract_access_token(
                request=request
            )
            if not access_token:
                raise HTTPException(
                    status_code=401, detail="Missing authentication token"
                )

            current_user = (
                await auth_manager.token_parser.user_details_from_access_token(
                    access_token=access_token
                )
            )

            sm.set_current_user(current_user)
            project = _extract_project_from_request(request)
            if project is not None:
                project_token = sm.set_current_project(project)
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(
                status_code=401, detail="Invalid or expired access token"
            )

    try:
        yield current_user
    finally:
        if sm is not None and project_token is not None:
            sm.reset_current_project(project_token)
