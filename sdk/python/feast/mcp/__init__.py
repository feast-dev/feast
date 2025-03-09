"""Model Context Protocol (MCP) integration for Feast.

This module provides MCP server implementation for Feast feature store,
allowing LLMs to interact with Feast through the Model Context Protocol.
"""

from feast.mcp.server import FeastMCP

__all__ = ["FeastMCP"]
