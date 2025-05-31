"""
MCP (Model Context Protocol) integration for Feast Feature Server.

This module provides MCP support for Feast by integrating with fastapi_mcp
to expose Feast functionality through the Model Context Protocol.
"""

import logging
from typing import Any, Dict, List, Optional

import feast
from feast.feature_store import FeatureStore

logger = logging.getLogger(__name__)

try:
    from fastapi_mcp import FastMCPIntegration
    from mcp.server.models import (
        InitializeRequest,
        Resource,
        ResourceTemplate,
        Tool,
    )
    from mcp.types import JSONRPCRequest, JSONRPCResponse
    MCP_AVAILABLE = True
except ImportError:
    logger.warning(
        "fastapi_mcp is not installed. MCP support will be disabled. "
        "Install it with: pip install fastapi_mcp"
    )
    MCP_AVAILABLE = False
    # Create placeholder classes for testing
    FastMCPIntegration = None


class FeastMCPServer:
    """MCP Server implementation for Feast."""

    def __init__(self, store: FeatureStore, server_name: str = "feast-mcp-server", version: str = "1.0.0"):
        self.store = store
        self.server_name = server_name
        self.version = version
        self._mcp_integration = None

        if not MCP_AVAILABLE:
            raise ImportError(
                "fastapi_mcp is required for MCP support. Install it with: pip install fastapi_mcp"
            )

    def create_mcp_integration(self, app) -> Optional["FastMCPIntegration"]:
        """Create and configure MCP integration with the FastAPI app."""
        if not MCP_AVAILABLE:
            logger.warning("MCP not available, skipping integration")
            return None

        try:
            # Initialize MCP integration
            mcp_integration = FastMCPIntegration(
                server_name=self.server_name,
                server_version=self.version
            )

            # Register MCP tools
            self._register_tools(mcp_integration)

            # Register MCP resources
            self._register_resources(mcp_integration)

            # Attach to FastAPI app
            mcp_integration.attach_to_app(app)

            self._mcp_integration = mcp_integration
            logger.info(f"MCP integration initialized for {self.server_name} v{self.version}")

            return mcp_integration

        except Exception as e:
            logger.error(f"Failed to initialize MCP integration: {e}")
            return None

    def _register_tools(self, mcp_integration: "FastMCPIntegration"):
        """Register MCP tools that clients can invoke."""

        @mcp_integration.tool()
        async def get_online_features(
            entities: Dict[str, List[Any]],
            features: Optional[List[str]] = None,
            feature_service: Optional[str] = None,
            full_feature_names: bool = False
        ) -> Dict[str, Any]:
            """
            Get online features from Feast feature store.
            
            Args:
                entities: Dictionary of entity values
                features: List of feature references (optional if feature_service is provided)
                feature_service: Name of feature service (optional if features is provided)
                full_feature_names: Whether to return full feature names
            
            Returns:
                Dictionary containing feature values
            """
            try:
                if feature_service:
                    fs = self.store.get_feature_service(feature_service)
                    result = self.store.get_online_features(
                        features=fs,
                        entity_rows=entities,
                        full_feature_names=full_feature_names
                    )
                else:
                    result = self.store.get_online_features(
                        features=features,
                        entity_rows=entities,
                        full_feature_names=full_feature_names
                    )

                return result.to_dict()
            except Exception as e:
                logger.error(f"Error getting online features: {e}")
                raise

        @mcp_integration.tool()
        async def list_feature_views() -> List[Dict[str, Any]]:
            """
            List all feature views in the feature store.
            
            Returns:
                List of feature view information
            """
            try:
                feature_views = self.store.list_feature_views()
                return [
                    {
                        "name": fv.name,
                        "features": [f.name for f in fv.features],
                        "entities": [e.name for e in fv.entities],
                        "description": getattr(fv, 'description', None),
                        "tags": getattr(fv, 'tags', {}),
                    }
                    for fv in feature_views
                ]
            except Exception as e:
                logger.error(f"Error listing feature views: {e}")
                raise

        @mcp_integration.tool()
        async def list_feature_services() -> List[Dict[str, Any]]:
            """
            List all feature services in the feature store.
            
            Returns:
                List of feature service information
            """
            try:
                feature_services = self.store.list_feature_services()
                return [
                    {
                        "name": fs.name,
                        "features": [
                            f"{proj.name}:{feat}"
                            for proj in fs.feature_view_projections
                            for feat in proj.features
                        ],
                        "description": getattr(fs, 'description', None),
                        "tags": getattr(fs, 'tags', {}),
                    }
                    for fs in feature_services
                ]
            except Exception as e:
                logger.error(f"Error listing feature services: {e}")
                raise

        @mcp_integration.tool()
        async def get_feature_store_info() -> Dict[str, Any]:
            """
            Get information about the Feast feature store.
            
            Returns:
                Dictionary containing feature store information
            """
            try:
                return {
                    "project": self.store.project,
                    "provider": self.store.config.provider,
                    "online_store_type": getattr(self.store.config.online_store, 'type', 'unknown'),
                    "registry_type": getattr(self.store.config.registry, 'registry_type', 'unknown'),
                    "version": feast.__version__,
                }
            except Exception as e:
                logger.error(f"Error getting feature store info: {e}")
                raise

    def _register_resources(self, mcp_integration: "FastMCPIntegration"):
        """Register MCP resources that clients can access."""

        @mcp_integration.resource("feast://feature-views")
        async def get_feature_views_resource() -> str:
            """
            Resource containing all feature views in JSON format.
            """
            try:
                feature_views = await self._get_tool_handler("list_feature_views")()
                import json
                return json.dumps(feature_views, indent=2)
            except Exception as e:
                logger.error(f"Error getting feature views resource: {e}")
                raise

        @mcp_integration.resource("feast://feature-services")
        async def get_feature_services_resource() -> str:
            """
            Resource containing all feature services in JSON format.
            """
            try:
                feature_services = await self._get_tool_handler("list_feature_services")()
                import json
                return json.dumps(feature_services, indent=2)
            except Exception as e:
                logger.error(f"Error getting feature services resource: {e}")
                raise

    def _get_tool_handler(self, tool_name: str):
        """Get a tool handler by name."""
        if self._mcp_integration and hasattr(self._mcp_integration, '_tools'):
            return self._mcp_integration._tools.get(tool_name)
        return None


def add_mcp_support_to_app(app, store: FeatureStore, config) -> Optional[FeastMCPServer]:
    """
    Add MCP support to a FastAPI application.
    
    Args:
        app: FastAPI application instance
        store: Feast FeatureStore instance
        config: MCP configuration
    
    Returns:
        FeastMCPServer instance if successful, None otherwise
    """
    if not config or not getattr(config, 'mcp_enabled', False):
        logger.info("MCP support is disabled")
        return None

    if not MCP_AVAILABLE:
        logger.warning("MCP support requested but fastapi_mcp is not available")
        return None

    try:
        server_name = getattr(config, 'mcp_server_name', 'feast-mcp-server')
        version = getattr(config, 'mcp_server_version', '1.0.0')

        mcp_server = FeastMCPServer(store, server_name, version)
        mcp_integration = mcp_server.create_mcp_integration(app)

        if mcp_integration:
            logger.info("MCP support successfully added to Feast feature server")
            return mcp_server
        else:
            logger.error("Failed to create MCP integration")
            return None

    except Exception as e:
        logger.error(f"Error adding MCP support: {e}")
        return None
