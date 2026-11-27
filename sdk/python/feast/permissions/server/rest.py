"""
A module with utility functions to support authorizing the REST servers using the FastAPI framework.
"""

from typing import Any, AsyncIterator, Callable, Optional

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


def build_auth_dependency(
    scope_to_query_project: bool = False,
) -> Callable[[Request], AsyncIterator[Any]]:
    """Build the FastAPI auth dependency that authenticates the caller and
    sets up the security-manager context for the request.

    Args:
        scope_to_query_project: Controls which project's permissions are
            checked for the request.

            ``False`` (default, safe): the request is authorized against the
            server's own project (``SecurityManager._project``, i.e.
            ``store.project``). Use this for every server that serves a single
            project and always acts on its own project regardless of the
            request, such as the feature server. The caller-supplied
            ``?project=`` query parameter must NOT be able to change which
            project's permissions are enforced here, otherwise a user with
            rights in some other project (or a non-existent one, which has no
            permissions and therefore grants all authenticated users access)
            could read and write this project's data.

            ``True``: the request is authorized against the project named in
            the ``project`` query parameter. Use this ONLY for servers that
            serve many projects and act on the requested project, i.e. the
            registry REST server and anything that mounts its routes (UI
            server). There the requested project is also the project whose
            data is touched, so checking its permissions is correct.

    Yields so any project context is reset after the request completes.
    """

    async def inject_user_details(request: Request) -> AsyncIterator[Any]:
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
                sm.set_current_request_token(access_token)
                if scope_to_query_project:
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

    return inject_user_details


inject_user_details = build_auth_dependency(scope_to_query_project=False)
