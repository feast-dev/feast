"""
MCP (Model Context Protocol) integration for Feast Feature Server.

This module provides MCP support for Feast by integrating with fastapi_mcp
to expose Feast functionality through the Model Context Protocol.
"""

import logging
from typing import Any, Dict, Optional, Set

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


def add_mcp_support_to_app(app, store: FeatureStore, config) -> Optional["FastApiMCP"]:
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
            # Defensive guard for programmatic callers.
            raise McpTransportNotSupportedError(
                f"Unsupported mcp_transport={transport!r}. Expected 'sse' or 'http'."
            )

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
