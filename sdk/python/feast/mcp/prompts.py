"""MCP prompts for Feast feature store."""

from typing import Dict, List, Any, Optional
import json

from mcp.server.fastmcp import FastMCP
from feast import FeatureStore

def register_feature_prompts(mcp: FastMCP, feature_store: FeatureStore):
    """Register Feast feature prompts with the MCP server.
    
    Args:
        mcp: The MCP server instance
        feature_store: The Feast feature store instance
    """
    
    @mcp.prompt("feast_feature_store_overview")
    def feature_store_overview() -> str:
        """Provides an overview of the Feast feature store."""
        feature_views = feature_store.list_feature_views()
        entities = feature_store.list_entities()
        feature_services = feature_store.list_feature_services()
        data_sources = feature_store.list_data_sources()
        
        overview = [
            f"# Feast Feature Store: {feature_store.project}",
            "",
            f"## Feature Views ({len(feature_views)})",
        ]
        
        for fv in feature_views:
            overview.append(f"- {fv.name}: {fv.description or 'No description'}")
        
        overview.extend([
            "",
            f"## Entities ({len(entities)})",
        ])
        
        for entity in entities:
            overview.append(f"- {entity.name}: {entity.description or 'No description'}")
        
        overview.extend([
            "",
            f"## Feature Services ({len(feature_services)})",
        ])
        
        for fs in feature_services:
            overview.append(f"- {fs.name}: {fs.description or 'No description'}")
        
        overview.extend([
            "",
            f"## Data Sources ({len(data_sources)})",
        ])
        
        for ds in data_sources:
            overview.append(f"- {ds.name}: {ds.description or 'No description'}")
        
        return "\n".join(overview)
    
    @mcp.prompt("feast_feature_retrieval_guide")
    def feature_retrieval_guide() -> str:
        """Provides a guide on how to retrieve features from the feature store."""
        return """
# Feast Feature Retrieval Guide

## Online Feature Retrieval

To retrieve online features, use the `get_online_features` tool with the following parameters:

- `entity_rows`: List of entity rows to retrieve features for
- `features`: List of feature references to retrieve
- `full_feature_names`: Whether to include the feature view name as a prefix

Example:
```python
response = get_online_features(
    entity_rows=[{"driver_id": 1001}],
    features=["driver_stats:conv_rate", "driver_stats:avg_daily_trips"],
    full_feature_names=False
)
```

## Feature View Schema

To understand the schema of a feature view, use the `get_feature_view_schema` tool:

```python
schema = get_feature_view_schema(feature_view_name="driver_stats")
```

## Feature Materialization

To materialize features for a feature view, use the `materialize_features` tool:

```python
result = materialize_features(
    feature_view_name="driver_stats",
    start_date="2023-01-01",
    end_date="2023-01-31"
)
```

For incremental materialization, use the `materialize_incremental` tool:

```python
result = materialize_incremental(
    end_date="2023-01-31",
    feature_views=["driver_stats", "customer_stats"]
)
```
"""
    
    @mcp.prompt("feast_feature_exploration")
    def feature_exploration() -> str:
        """Provides guidance on how to explore the feature store."""
        return """
# Feast Feature Store Exploration Guide

## Available Resources

The following resources are available to explore the feature store:

- `feast://feature-views`: List all feature views
- `feast://feature-view/{name}`: Get details of a specific feature view
- `feast://entities`: List all entities
- `feast://entity/{name}`: Get details of a specific entity
- `feast://feature-services`: List all feature services
- `feast://feature-service/{name}`: Get details of a specific feature service
- `feast://data-sources`: List all data sources
- `feast://data-source/{name}`: Get details of a specific data source

## Example Exploration Flow

1. Start by getting an overview of the feature store:
   - Use the `feast_feature_store_overview` prompt

2. Explore available feature views:
   - Use the `feast://feature-views` resource

3. Examine a specific feature view:
   - Use the `feast://feature-view/{name}` resource with the name of the feature view

4. Understand the entities:
   - Use the `feast://entities` resource
   - Examine specific entities with `feast://entity/{name}`

5. Explore feature services:
   - Use the `feast://feature-services` resource
   - Examine specific feature services with `feast://feature-service/{name}`

6. Retrieve feature values:
   - Use the `get_online_features` tool to retrieve feature values for specific entities
"""
