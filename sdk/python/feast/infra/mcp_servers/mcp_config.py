from typing import Literal

from pydantic import StrictBool, StrictStr

from feast.infra.feature_servers.base_config import BaseFeatureServerConfig


class McpFeatureServerConfig(BaseFeatureServerConfig):
    """MCP (Model Context Protocol) Feature Server configuration."""

    type: Literal["mcp"] = "mcp"

    mcp_enabled: StrictBool = False

    mcp_server_name: StrictStr = "feast-mcp-server"

    mcp_server_version: StrictStr = "1.0.0"

    mcp_transport: Literal["sse", "http"] = "sse"

    mcp_base_path: StrictStr = "/mcp"

    transformation_service_endpoint: StrictStr = "localhost:6566"
