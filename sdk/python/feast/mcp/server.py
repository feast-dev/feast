"""MCP server implementation for Feast feature store."""

from mcp.server.fastmcp import FastMCP

from feast import FeatureStore


class FeastMCP(FastMCP):
    """MCP server implementation for Feast feature store.

    This class extends the FastMCP server to provide MCP capabilities
    for a Feast feature store. It registers resources and tools that
    allow LLMs to interact with the feature store through the Model
    Context Protocol.
    """

    def __init__(
        self,
        feature_store: FeatureStore,
        name: str = "Feast MCP Server",
        **kwargs
    ):
        """Initialize the Feast MCP server.

        Args:
            feature_store: The Feast feature store instance
            name: Name of the MCP server
            **kwargs: Additional arguments to pass to FastMCP
        """
        super().__init__(name=name, **kwargs)
        self.feature_store = feature_store

        # Register resources and tools
        self._register_resources()
        self._register_tools()
        self._register_prompts()

    def _register_resources(self):
        """Register Feast resources with the MCP server."""
        from feast.mcp.resources import register_feature_resources
        register_feature_resources(self, self.feature_store)

    def _register_tools(self):
        """Register Feast tools with the MCP server."""
        from feast.mcp.tools import register_feature_tools
        register_feature_tools(self, self.feature_store)

    def _register_prompts(self):
        """Register Feast prompts with the MCP server."""
        from feast.mcp.prompts import register_feature_prompts
        register_feature_prompts(self, self.feature_store)

    @property
    def app(self):
        """Get the FastAPI app instance.

        Returns:
            The FastAPI app instance
        """
        return self.fastapi_app
