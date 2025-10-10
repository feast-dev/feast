from typing import Literal, Optional

from pydantic import StrictBool, StrictStr

from feast.infra.feature_servers.base_config import BaseFeatureServerConfig


class McpFeatureServerConfig(BaseFeatureServerConfig):
    """MCP (Model Context Protocol) Feature Server configuration."""

    # Feature server type selector
    type: Literal["mcp"] = "mcp"

    # Enable MCP server support - defaults to False as requested
    mcp_enabled: StrictBool = False

    # MCP server name for identification
    mcp_server_name: StrictStr = "feast-mcp-server"

    # MCP server version
    mcp_server_version: StrictStr = "1.0.0"

    # Optional MCP transport configuration
    mcp_transport: Optional[StrictStr] = None

    # The endpoint definition for transformation_service (inherited from base)
    transformation_service_endpoint: StrictStr = "localhost:6566"
