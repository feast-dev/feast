"""
MCP (Model Context Protocol) integration for Feast Feature Server.

This module provides MCP support for Feast by integrating with fastapi_mcp
to expose Feast functionality through the Model Context Protocol.
"""

import logging
from typing import Literal, Optional

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
        # Create MCP server from the FastAPI app
        mcp = FastApiMCP(
            app,
            name=getattr(config, "mcp_server_name", "feast-feature-store"),
            description="Feast Feature Store MCP Server - Access feature store data and operations through MCP",
        )

        transport: Literal["sse", "http"] = getattr(config, "mcp_transport", "sse") or "sse"
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
                mcp.mount()
        else:
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
        logger.error(f"Failed to initialize MCP integration: {e}")
        return None
