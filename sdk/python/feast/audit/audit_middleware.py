# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
FastAPI middleware for structured audit logging of REST endpoints.

``AuditLoggingMiddleware`` logs every HTTP request/response on the REST
endpoints (``/get-online-features``, ``/push``, etc.) with principal,
resource, outcome, and duration.

MCP tool-call auditing is handled at the protocol layer by wrapping
the ``tools/call`` handler inside ``add_mcp_support_to_app()`` (see
``feast.infra.mcp_servers.mcp_server``).

The middleware is added only when ``audit_logging.enabled`` is ``true``
in the feature-server configuration.
"""

import logging
import time
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from feast.audit.audit_logger import (
    AuditLogger,
    AuditPrincipal,
    AuditResource,
    AuditSource,
)

logger = logging.getLogger(__name__)

# REST paths that correspond to read/write actions
_PATH_RESOURCE_MAP = {
    "/get-online-features": ("feature_service", ["READ_ONLINE"]),
    "/retrieve-online-documents": ("feature_service", ["READ_ONLINE"]),
    "/push": ("push_source", ["WRITE_ONLINE", "WRITE_OFFLINE"]),
    "/write-to-online-store": ("feature_view", ["WRITE_ONLINE"]),
    "/materialize": ("feature_view", ["WRITE_ONLINE"]),
    "/materialize-incremental": ("feature_view", ["WRITE_ONLINE"]),
}


def _extract_client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    if request.client:
        return request.client.host
    return ""


def _principal_from_request(request: Request) -> AuditPrincipal:
    """Build an ``AuditPrincipal`` from the security manager's current user."""
    try:
        from feast.permissions.security_manager import get_security_manager

        sm = get_security_manager()
        if sm and sm.current_user:
            user = sm.current_user
            return AuditPrincipal(
                username=user.username,
                roles=list(user.roles) if user.roles else [],
                auth_type=request.headers.get("x-feast-auth-type", ""),
            )
    except Exception:
        pass
    return AuditPrincipal()


class AuditLoggingMiddleware(BaseHTTPMiddleware):
    """Emit ``http.request`` audit events for REST endpoints."""

    async def dispatch(self, request: Request, call_next):  # type: ignore[override]
        audit: Optional[AuditLogger] = getattr(request.app.state, "audit_logger", None)
        if audit is None:
            return await call_next(request)

        path = request.url.path
        # Skip health and static endpoints
        if path in ("/health", "/docs", "/openapi.json") or path.startswith("/static"):
            return await call_next(request)

        # Skip MCP endpoints — handled by McpAuditMiddleware
        if path.startswith("/mcp"):
            return await call_next(request)

        request_id = request.headers.get("x-request-id", audit.new_request_id())
        request.state.audit_request_id = request_id

        start = time.monotonic()
        response: Response
        outcome = "success"
        status_code = 200
        try:
            response = await call_next(request)
            status_code = response.status_code
            if status_code >= 400:
                outcome = "failure"
        except Exception:
            outcome = "error"
            status_code = 500
            raise
        finally:
            duration_ms = (time.monotonic() - start) * 1000.0
            resource_info = _PATH_RESOURCE_MAP.get(path, ("", []))
            audit.log_http_request(
                request_id=request_id,
                method=request.method,
                path=path,
                principal=_principal_from_request(request),
                source=AuditSource(ip=_extract_client_ip(request), transport="http"),
                resource=AuditResource(
                    type=resource_info[0], actions=list(resource_info[1])
                ),
                outcome=outcome,
                duration_ms=round(duration_ms, 2),
                status_code=status_code,
            )

        return response
