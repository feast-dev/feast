"""Standalone Model Context Protocol server for Feast.

Serves MCP in its own process (``feast mcp``), proxying to a running Feast
feature server and/or REST registry server over HTTP. Distinct from
:mod:`feast.infra.mcp_servers`, which mounts an OpenAPI-derived MCP endpoint
*inside* the feature server via ``fastapi_mcp``.

Requires the optional ``mcp-server`` extra (``feast[mcp-server]``).
"""
