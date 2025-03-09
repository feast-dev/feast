"""MCP resources for Feast feature store."""

import json

from mcp.server.fastmcp import FastMCP
from feast import FeatureStore


def register_feature_resources(mcp: FastMCP, feature_store: FeatureStore):
    """Register Feast feature resources with the MCP server.

    Args:
        mcp: The MCP server instance
        feature_store: The Feast feature store instance
    """

    @mcp.resource("feast://feature-views")
    def list_feature_views() -> str:
        """List all available feature views."""
        feature_views = feature_store.list_feature_views()
        return "\n".join([f"- {fv.name}: {fv.description or 'No description'}" for fv in feature_views])

    @mcp.resource("feast://feature-view/{name}")
    def get_feature_view(name: str) -> str:
        """Get details of a specific feature view."""
        try:
            fv = feature_store.get_feature_view(name)
            features = [f.name for f in fv.features]
            entities = [e for e in fv.entities]

            return (
                f"Feature View: {fv.name}\n"
                f"Description: {fv.description or 'No description'}\n"
                f"Entities: {', '.join(entities)}\n"
                f"Features: {', '.join(features)}\n"
                f"TTL: {fv.ttl}\n"
                f"Online: {fv.online}\n"
                f"Tags: {json.dumps(fv.tags) if fv.tags else '{}'}"
            )
        except Exception as e:
            return f"Error retrieving feature view {name}: {str(e)}"

    @mcp.resource("feast://entities")
    def list_entities() -> str:
        """List all available entities."""
        entities = feature_store.list_entities()
        return "\n".join([f"- {entity.name}: {entity.description or 'No description'}" for entity in entities])

    @mcp.resource("feast://entity/{name}")
    def get_entity(name: str) -> str:
        """Get details of a specific entity."""
        try:
            entity = feature_store.get_entity(name)
            return (
                f"Entity: {entity.name}\n"
                f"Description: {entity.description or 'No description'}\n"
                f"Value Type: {entity.value_type}\n"
                f"Join Key: {entity.join_key}\n"
                f"Tags: {json.dumps(entity.tags) if entity.tags else '{}'}"
            )
        except Exception as e:
            return f"Error retrieving entity {name}: {str(e)}"

    @mcp.resource("feast://feature-services")
    def list_feature_services() -> str:
        """List all available feature services."""
        feature_services = feature_store.list_feature_services()
        return "\n".join([f"- {fs.name}: {fs.description or 'No description'}" for fs in feature_services])

    @mcp.resource("feast://feature-service/{name}")
    def get_feature_service(name: str) -> str:
        """Get details of a specific feature service."""
        try:
            fs = feature_store.get_feature_service(name)
            feature_view_projections = []
            for projection in fs.feature_view_projections:
                features = [f.name for f in projection.features] if projection.features else []
                feature_view_projections.append(
                    f"  - {projection.name}: {', '.join(features)}"
                )

            return (
                f"Feature Service: {fs.name}\n"
                f"Description: {fs.description or 'No description'}\n"
                f"Feature Views:\n"
                f"{chr(10).join(feature_view_projections)}\n"
                f"Tags: {json.dumps(fs.tags) if fs.tags else '{}'}"
            )
        except Exception as e:
            return f"Error retrieving feature service {name}: {str(e)}"

    @mcp.resource("feast://data-sources")
    def list_data_sources() -> str:
        """List all available data sources."""
        data_sources = feature_store.list_data_sources()
        return "\n".join([f"- {ds.name}: {ds.description or 'No description'}" for ds in data_sources])

    @mcp.resource("feast://data-source/{name}")
    def get_data_source(name: str) -> str:
        """Get details of a specific data source."""
        try:
            ds = feature_store.get_data_source(name)
            return (
                f"Data Source: {ds.name}\n"
                f"Description: {ds.description or 'No description'}\n"
                f"Type: {type(ds).__name__}\n"
                f"Tags: {json.dumps(ds.tags) if ds.tags else '{}'}"
            )
        except Exception as e:
            return f"Error retrieving data source {name}: {str(e)}"
