"""Registry MCP sub-server.

Exposes read-only tools that proxy to the Feast REST registry server
at ``/api/v1/``, letting an LLM discover what features, entities,
feature views, and feature services exist in a Feast project.
"""

from typing import Any, Dict, Optional

from fastmcp import FastMCP

from feast.mcp.auth import get_auth_token
from feast.mcp.client import FeastClient, path_segment


def create_registry_mcp(client: FeastClient) -> FastMCP:
    registry_mcp = FastMCP("feast-registry")

    # ------------------------------------------------------------------
    # Projects
    # ------------------------------------------------------------------

    @registry_mcp.tool
    async def list_projects() -> Any:
        """List all Feast projects in the registry.

        Use this first to discover available projects before querying
        other registry objects.
        """
        return await client.request("GET", "/api/v1/projects", token=get_auth_token())

    @registry_mcp.tool
    async def get_project(name: str) -> Any:
        """Get details of a specific Feast project.

        Args:
            name: Project name.
        """
        return await client.request(
            "GET", f"/api/v1/projects/{path_segment(name)}", token=get_auth_token()
        )

    # ------------------------------------------------------------------
    # Entities
    # ------------------------------------------------------------------

    @registry_mcp.tool
    async def list_entities(project: str) -> Any:
        """List all entities in a project.

        Entities define the join keys used to look up feature values
        (e.g. customer_id, driver_id).

        Args:
            project: Feast project name.
        """
        return await client.request(
            "GET",
            "/api/v1/entities",
            token=get_auth_token(),
            params={"project": project},
        )

    @registry_mcp.tool
    async def get_entity(name: str, project: str) -> Any:
        """Get details of a specific entity.

        Args:
            name: Entity name.
            project: Feast project name.
        """
        return await client.request(
            "GET",
            f"/api/v1/entities/{path_segment(name)}",
            token=get_auth_token(),
            params={"project": project},
        )

    # ------------------------------------------------------------------
    # Feature Views (all types: regular, stream, on-demand)
    # ------------------------------------------------------------------

    @registry_mcp.tool
    async def list_feature_views(
        project: str,
        entity: Optional[str] = None,
        feature: Optional[str] = None,
        feature_service: Optional[str] = None,
        data_source: Optional[str] = None,
    ) -> Any:
        """List all feature views in a project.

        Returns regular, stream, and on-demand feature views.
        Use the optional filters to narrow results.

        Args:
            project: Feast project name.
            entity: Filter by entity name.
            feature: Filter by feature name.
            feature_service: Filter by feature service name.
            data_source: Filter by data source name.
        """
        params: Dict[str, str] = {"project": project}
        if entity is not None:
            params["entity"] = entity
        if feature is not None:
            params["feature"] = feature
        if feature_service is not None:
            params["feature_service"] = feature_service
        if data_source is not None:
            params["data_source"] = data_source
        return await client.request(
            "GET",
            "/api/v1/feature_views",
            token=get_auth_token(),
            params=params,
        )

    @registry_mcp.tool
    async def get_feature_view(name: str, project: str) -> Any:
        """Get details of a specific feature view.

        Returns the feature view regardless of type (regular, stream,
        or on-demand), including its schema, entities, and data source.

        Args:
            name: Feature view name.
            project: Feast project name.
        """
        return await client.request(
            "GET",
            f"/api/v1/feature_views/{path_segment(name)}",
            token=get_auth_token(),
            params={"project": project},
        )

    # ------------------------------------------------------------------
    # Features (individual columns within feature views)
    # ------------------------------------------------------------------

    @registry_mcp.tool
    async def list_features(
        project: str,
        feature_view: Optional[str] = None,
    ) -> Any:
        """List individual features (columns) across all feature views.

        Args:
            project: Feast project name.
            feature_view: Filter by feature view name.
        """
        params: Dict[str, str] = {"project": project}
        if feature_view is not None:
            params["feature_view"] = feature_view
        return await client.request(
            "GET",
            "/api/v1/features",
            token=get_auth_token(),
            params=params,
        )

    # ------------------------------------------------------------------
    # Feature Services
    # ------------------------------------------------------------------

    @registry_mcp.tool
    async def list_feature_services(
        project: str,
        feature_view: Optional[str] = None,
    ) -> Any:
        """List all feature services in a project.

        A feature service groups multiple feature views together for
        serving as a single unit.

        Args:
            project: Feast project name.
            feature_view: Filter by feature view name.
        """
        params: Dict[str, str] = {"project": project}
        if feature_view is not None:
            params["feature_view"] = feature_view
        return await client.request(
            "GET",
            "/api/v1/feature_services",
            token=get_auth_token(),
            params=params,
        )

    @registry_mcp.tool
    async def get_feature_service(name: str, project: str) -> Any:
        """Get details of a specific feature service.

        Args:
            name: Feature service name.
            project: Feast project name.
        """
        return await client.request(
            "GET",
            f"/api/v1/feature_services/{path_segment(name)}",
            token=get_auth_token(),
            params={"project": project},
        )

    # ------------------------------------------------------------------
    # Data Sources
    # ------------------------------------------------------------------

    @registry_mcp.tool
    async def list_data_sources(project: str) -> Any:
        """List all data sources in a project.

        Data sources define where feature data comes from (e.g.
        BigQuery tables, Parquet files, Kafka topics).

        Args:
            project: Feast project name.
        """
        return await client.request(
            "GET",
            "/api/v1/data_sources",
            token=get_auth_token(),
            params={"project": project},
        )

    @registry_mcp.tool
    async def get_data_source(name: str, project: str) -> Any:
        """Get details of a specific data source.

        Args:
            name: Data source name.
            project: Feast project name.
        """
        return await client.request(
            "GET",
            f"/api/v1/data_sources/{path_segment(name)}",
            token=get_auth_token(),
            params={"project": project},
        )

    # ------------------------------------------------------------------
    # Search & Lineage
    # ------------------------------------------------------------------

    @registry_mcp.tool
    async def search_registry(query: str, project: Optional[str] = None) -> Any:
        """Full-text search across all registry objects.

        Searches entities, feature views, feature services, data
        sources, and other objects by name and metadata.

        Args:
            query: Search query string.
            project: Optionally scope search to a project.
        """
        params: Dict[str, str] = {"query": query}
        if project is not None:
            params["project"] = project
        return await client.request(
            "GET",
            "/api/v1/search",
            token=get_auth_token(),
            params=params,
        )

    @registry_mcp.tool
    async def get_lineage(
        project: str,
        object_type: Optional[str] = None,
        object_name: Optional[str] = None,
    ) -> Any:
        """Get lineage relationships between registry objects.

        Without filters, returns the complete lineage graph for the
        project. With object_type and object_name, returns
        relationships for a specific object.

        Args:
            project: Feast project name.
            object_type: Filter by object type (e.g. 'feature_view', 'entity').
            object_name: Filter by object name.
        """
        if object_type and object_name:
            return await client.request(
                "GET",
                f"/api/v1/lineage/objects/{path_segment(object_type)}/{path_segment(object_name)}",
                token=get_auth_token(),
                params={"project": project},
            )
        return await client.request(
            "GET",
            "/api/v1/lineage/complete",
            token=get_auth_token(),
            params={"project": project},
        )

    return registry_mcp
