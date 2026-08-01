"""
MCP (Model Context Protocol) integration for Feast Feature Server.

This module provides MCP support for Feast by integrating with fastapi_mcp
to expose Feast functionality through the Model Context Protocol.

When audit logging is enabled, the ``CallToolRequest`` handler on the
low-level MCP ``Server`` is wrapped so that every tool invocation is
logged with typed tool name, outcome, duration, and principal — without
parsing raw JSON-RPC bodies.
"""

import logging
import time
from typing import TYPE_CHECKING, Any, Dict, Optional, Set

if TYPE_CHECKING:
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
    FastApiMCP = None

try:
    from mcp.types import CallToolRequest as _CallToolRequest
except ImportError:
    _CallToolRequest = None  # type: ignore[assignment,misc]


class McpTransportNotSupportedError(RuntimeError):
    pass


def _resolve_schema_references_safe(
    schema_part: Dict[str, Any],
    reference_schema: Dict[str, Any],
    _seen_refs: Optional[Set[str]] = None,
) -> Dict[str, Any]:
    """Resolve ``$ref`` pointers in an OpenAPI schema **without** infinite recursion.

    ``fastapi_mcp`` <=0.4.0 ships a ``resolve_schema_references`` helper that
    inlines every ``$ref`` it encounters, but never tracks which refs have
    already been visited.  Feast's OpenAPI schema contains self-referential
    types (protobuf ``Value`` -> ``Struct`` -> ``Value``), which causes a
    ``RecursionError``.

    This replacement keeps a *seen-refs* set and replaces any circular
    ``$ref`` with an empty object schema instead of recursing forever.

    Reference: ``fastapi_mcp/openapi/utils.py`` lines 19-55
    https://github.com/tadata-org/fastapi_mcp/blob/main/fastapi_mcp/openapi/utils.py#L19-L55
    """
    if _seen_refs is None:
        _seen_refs = set()

    schema_part = schema_part.copy()

    if "$ref" in schema_part:
        ref_path = schema_part["$ref"]
        if ref_path in _seen_refs:
            schema_part.pop("$ref")
            schema_part.setdefault("type", "object")
            return schema_part
        if ref_path.startswith("#/components/schemas/"):
            model_name = ref_path.split("/")[-1]
            schemas = reference_schema.get("components", {}).get("schemas", {})
            if model_name in schemas:
                _seen_refs = _seen_refs | {ref_path}
                ref_schema = schemas[model_name].copy()
                schema_part.pop("$ref")
                schema_part.update(ref_schema)

    for key, value in schema_part.items():
        if isinstance(value, dict):
            schema_part[key] = _resolve_schema_references_safe(
                value, reference_schema, _seen_refs
            )
        elif isinstance(value, list):
            schema_part[key] = [
                _resolve_schema_references_safe(item, reference_schema, _seen_refs)
                if isinstance(item, dict)
                else item
                for item in value
            ]

    return schema_part


def _patch_fastapi_mcp_schema_resolver() -> None:
    """Monkey-patch ``fastapi_mcp.openapi.utils.resolve_schema_references``
    with our circular-ref-safe version so that ``FastApiMCP`` can process
    Feast's OpenAPI schema without hitting a ``RecursionError``."""
    try:
        import fastapi_mcp.openapi.utils as _mcp_utils

        _mcp_utils.resolve_schema_references = _resolve_schema_references_safe  # type: ignore[assignment]
    except (ImportError, AttributeError):
        pass


def add_mcp_support_to_app(
    app,
    store: "FeatureStore",
    config,
    audit_logger: Optional[Any] = None,
) -> Optional["FastApiMCP"]:
    """Add MCP support to the FastAPI app if enabled in configuration."""
    if not MCP_AVAILABLE:
        logger.warning("MCP support requested but fastapi_mcp is not available")
        return None

    try:
        _patch_fastapi_mcp_schema_resolver()

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


def _get_call_tool_handler_key() -> Any:
    """Return the dict key used for CallToolRequest in ``server.request_handlers``.

    mcp 1.x uses the ``CallToolRequest`` *class* as the key in
    ``server.request_handlers``.
    """
    if _CallToolRequest is not None:
        return _CallToolRequest
    return None


def _principal_from_mcp_context(server: Any) -> Any:
    """Extract an ``AuditPrincipal`` from the MCP server's request context.

    In mcp 1.x the request context is a ``ContextVar`` accessed via
    ``server.request_context``. The ``.request`` attribute carries the
    original Starlette/FastAPI ``Request`` that ``fastapi_mcp`` injects
    through ``ServerMessageMetadata(request_context=request)``.
    """
    from feast.audit.audit_logger import AuditPrincipal

    try:
        ctx = server.request_context
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
    """Wrap the MCP server's ``CallToolRequest`` handler with audit logging.

    In mcp 1.x the handler lives at
    ``server.request_handlers[CallToolRequest]`` and has the signature
    ``async def handler(req: CallToolRequest) -> ServerResult``.  The
    JSON-RPC request_id is available on ``server.request_context``.
    """
    from feast.audit.audit_logger import AuditAction, AuditEvent, AuditSource

    handler_key = _get_call_tool_handler_key()
    handlers = getattr(mcp.server, "request_handlers", None)
    if handlers is None:
        logger.warning("Cannot wrap MCP call_tool handler: request_handlers not found")
        return

    if handler_key is None or handler_key not in handlers:
        logger.debug("No CallToolRequest handler registered; skipping audit wrapper")
        return

    original = handlers[handler_key]

    async def audited_call_tool(req: Any) -> Any:
        from feast.audit.audit_logger import mcp_audit_request_id

        params = getattr(req, "params", None)
        tool_name = getattr(params, "name", "") if params else ""
        request_id = audit.new_request_id()

        jsonrpc_id: Optional[str] = None
        try:
            ctx = mcp.server.request_context
            if hasattr(ctx, "request_id"):
                jsonrpc_id = str(ctx.request_id)
        except LookupError:
            pass

        token = mcp_audit_request_id.set(request_id)
        start = time.monotonic()
        outcome = "success"
        error_detail = ""
        try:
            result = await original(req)
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
            principal = _principal_from_mcp_context(mcp.server)
            audit.log(
                AuditEvent(
                    event_type="mcp.tools.call",
                    request_id=request_id,
                    jsonrpc_id=jsonrpc_id,
                    principal=principal,
                    source=AuditSource(transport="mcp-http"),
                    action=AuditAction(mcp_tool=tool_name),
                    outcome=outcome,
                    duration_ms=round(duration_ms, 2),
                    detail=error_detail,
                )
            )

    handlers[handler_key] = audited_call_tool
