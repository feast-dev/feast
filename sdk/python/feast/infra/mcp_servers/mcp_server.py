"""
MCP (Model Context Protocol) integration for Feast Feature Server.

This module provides MCP support for Feast by integrating with fastapi_mcp
to expose Feast functionality through the Model Context Protocol.
"""

import logging
from typing import Optional

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


def add_mcp_support_to_app(app, store: FeatureStore, config) -> Optional["FastApiMCP"]:
    """Add MCP support to the FastAPI app if enabled in configuration."""
    if not MCP_AVAILABLE:
        logger.warning("MCP support requested but fastapi_mcp is not available")
        return None

    try:
        # Create MCP server from the FastAPI app.
        # Forward mcp-session-id so endpoint handlers can tag spans with it.
        mcp = FastApiMCP(
            app,
            name=getattr(config, "mcp_server_name", "feast-feature-store"),
            description="Feast Feature Store MCP Server - Access feature store data and operations through MCP",
            headers=["authorization", "mcp-session-id"],
        )

        # Instrument the internal httpx client with OTEL so that trace
        # context propagates from the /mcp server span into the internal
        # ASGI calls to the actual FastAPI endpoints (Tier 3 enabler).
        _instrument_mcp_http_client(mcp)

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


def _instrument_mcp_http_client(mcp: "FastApiMCP") -> None:
    """Instrument fastapi_mcp's internal httpx client with OTEL.

    This ensures that when fastapi_mcp makes internal ASGI calls to
    the actual FastAPI endpoints, the current OTEL trace context
    (from the /mcp server span) is propagated via traceparent headers.
    Without this, the endpoint spans would be orphaned from the
    incoming trace.
    """
    try:
        from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

        http_client = getattr(mcp, "_http_client", None)
        if http_client is not None:
            HTTPXClientInstrumentor.instrument_client(http_client)
            logger.info("MCP internal httpx client instrumented for trace propagation")
        else:
            logger.debug("Could not access fastapi_mcp internal httpx client")
    except ImportError:
        logger.debug(
            "opentelemetry-instrumentation-httpx not installed; "
            "internal trace propagation disabled"
        )
