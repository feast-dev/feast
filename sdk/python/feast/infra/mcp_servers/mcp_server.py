"""
MCP (Model Context Protocol) integration for Feast Feature Server.

This module provides MCP support for Feast by integrating with fastapi_mcp
to expose Feast functionality through the Model Context Protocol.

When audit logging is enabled, the ``tools/call`` handler on the low-level
MCP ``Server`` is wrapped so that every tool invocation is logged with
typed tool name, outcome, duration, and principal — without parsing raw
JSON-RPC bodies.
"""

import logging
import time
from typing import Any, Optional

from feast.feature_store import FeatureStore

logger = logging.getLogger(__name__)

try:
    from fastapi_mcp import FastApiMCP

    MCP_AVAILABLE = True
except ImportError:
    logger.warning(
        "fastapi_mcp is not installed. MCP support will be disabled. "
        "Install it with: pip install fastapi_mcp"
    )
    MCP_AVAILABLE = False
    # Create placeholder classes for testing
    FastApiMCP = None


class McpTransportNotSupportedError(RuntimeError):
    pass


def add_mcp_support_to_app(
    app,
    store: FeatureStore,
    config,
    audit_logger: Optional[Any] = None,
) -> Optional["FastApiMCP"]:
    """Add MCP support to the FastAPI app if enabled in configuration."""
    if not MCP_AVAILABLE:
        logger.warning("MCP support requested but fastapi_mcp is not available")
        return None

    try:
        # Create MCP server from the FastAPI app
        mcp = FastApiMCP(
            app,
            name=getattr(config, "mcp_server_name", "feast-feature-store"),
            description="Feast Feature Store MCP Server - Access feature store data and operations through MCP",
        )

        transport = getattr(config, "mcp_transport", "sse")
        if transport == "http":
            mount_http = getattr(mcp, "mount_http", None)
            if mount_http is None:
                raise McpTransportNotSupportedError(
                    "mcp_transport=http requires fastapi_mcp with FastApiMCP.mount_http(). "
                    "Upgrade fastapi_mcp (or install feast[mcp]) to a newer version."
                )
            mount_http()
        elif transport == "sse":
            mount_sse = getattr(mcp, "mount_sse", None)
            if mount_sse is not None:
                mount_sse()
            else:
                logger.warning(
                    "transport sse not supported, fallback to the deprecated mount()."
                )
                mcp.mount()
        else:
            # Defensive guard for programmatic callers.
            raise McpTransportNotSupportedError(
                f"Unsupported mcp_transport={transport!r}. Expected 'sse' or 'http'."
            )

        if audit_logger is not None:
            _wrap_call_tool_handler(mcp, audit_logger)

        logger.info(
            "MCP support has been enabled for the Feast feature server at /mcp endpoint"
        )
        logger.info(
            f"MCP integration initialized for {getattr(config, 'mcp_server_name', 'feast-feature-store')} "
            f"v{getattr(config, 'mcp_server_version', '1.0.0')}"
        )

        return mcp

    except McpTransportNotSupportedError:
        raise
    except Exception as e:
        logger.error(f"Failed to initialize MCP integration: {e}", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Audit-logging wrapper for the MCP tools/call handler
# ---------------------------------------------------------------------------


def _principal_from_mcp_context(ctx: Any) -> Any:
    """Extract an ``AuditPrincipal`` from the MCP request context's HTTP headers.

    Unlike REST endpoints the ``SecurityManager`` ``ContextVar`` is never
    populated for MCP requests, so we read directly from the HTTP headers
    that ``fastapi_mcp`` forwards into the request context.
    """
    from feast.audit.audit_logger import AuditPrincipal

    try:
        request = getattr(ctx, "request", None)
        if request is None:
            return AuditPrincipal()
        headers: dict[str, str] = {}
        if hasattr(request, "headers"):
            headers = dict(request.headers)
        auth_type = headers.get("x-feast-auth-type", "")
        has_auth = bool(headers.get("authorization", ""))
        return AuditPrincipal(
            username="(authenticated)" if has_auth else "",
            auth_type=auth_type,
        )
    except Exception:
        return AuditPrincipal()


def _wrap_call_tool_handler(mcp: "FastApiMCP", audit: Any) -> None:
    """Wrap the MCP server's ``tools/call`` handler with audit logging.

    Operates at the protocol layer so that ``tool_name`` and error status
    come as typed Python objects — no JSON-RPC body parsing required.
    """
    from feast.audit.audit_logger import AuditAction, AuditEvent, AuditSource

    handlers = getattr(mcp.server, "_request_handlers", None)
    if handlers is None:
        logger.warning("Cannot wrap MCP call_tool handler: _request_handlers not found")
        return

    original = handlers.get("tools/call")
    if original is None:
        logger.debug("No tools/call handler registered; skipping audit wrapper")
        return

    async def audited_call_tool(ctx: Any, params: Any) -> Any:
        from feast.audit.audit_logger import mcp_audit_request_id

        tool_name = getattr(params, "name", "") if params else ""
        request_id = audit.new_request_id()
        jsonrpc_id: Optional[str] = None
        if hasattr(ctx, "request_id"):
            jsonrpc_id = str(ctx.request_id)

        # Propagate request_id so the internal REST call logged by
        # AuditLoggingMiddleware uses the same identifier.
        token = mcp_audit_request_id.set(request_id)
        start = time.monotonic()
        outcome = "success"
        error_detail = ""
        try:
            result = await original(ctx, params)
            if hasattr(result, "isError") and result.isError:
                outcome = "mcp_error"
            return result
        except Exception as exc:
            outcome = "error"
            error_detail = str(exc)[:200]
            raise
        finally:
            duration_ms = (time.monotonic() - start) * 1000.0
            mcp_audit_request_id.reset(token)
            audit.log(
                AuditEvent(
                    event_type="mcp.tools.call",
                    request_id=request_id,
                    jsonrpc_id=jsonrpc_id,
                    principal=_principal_from_mcp_context(ctx),
                    source=AuditSource(transport="mcp-http"),
                    action=AuditAction(mcp_tool=tool_name),
                    outcome=outcome,
                    duration_ms=round(duration_ms, 2),
                    detail=error_detail,
                )
            )

    handlers["tools/call"] = audited_call_tool
