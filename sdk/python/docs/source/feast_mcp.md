# Feast MCP Integration

Feast MCP integration allows LLMs to interact with the Feast feature store through the Model Context Protocol (MCP).

## Overview

The Model Context Protocol (MCP) is a standardized protocol for LLMs to interact with external systems. Feast MCP integration provides a server that implements the MCP protocol, allowing LLMs to:

- Explore feature views, entities, and feature services
- Retrieve feature values
- Materialize features
- Push features to the feature store

## Installation

To use the Feast MCP integration, you need to install the `mcp` package:

```bash
pip install mcp
```

## Usage

### Starting the MCP Server

To start the MCP server, use the `serve_mcp` method of the `FeatureStore` class:

```python
from feast import FeatureStore

# Initialize the feature store
fs = FeatureStore(repo_path="path/to/feature/repo")

# Start the MCP server
fs.serve_mcp(
    host="localhost",
    port=8000
)
```

### Connecting to the MCP Server

LLMs can connect to the MCP server using the MCP client:

```python
from mcp.client import MCPClient

# Connect to the MCP server
client = MCPClient(url="http://localhost:8000/mcp")

# Use the client to interact with the feature store
response = client.invoke_tool("get_online_features", {
    "entity_rows": [{"driver_id": 1001}],
    "features": ["driver_stats:conv_rate", "driver_stats:avg_daily_trips"],
    "full_feature_names": False
})
```

## Available Resources

The MCP server provides the following resources:

- `feast://feature-views`: List all feature views
- `feast://feature-view/{name}`: Get details of a specific feature view
- `feast://entities`: List all entities
- `feast://entity/{name}`: Get details of a specific entity
- `feast://feature-services`: List all feature services
- `feast://feature-service/{name}`: Get details of a specific feature service
- `feast://data-sources`: List all data sources
- `feast://data-source/{name}`: Get details of a specific data source

## Available Tools

The MCP server provides the following tools:

- `get_online_features`: Retrieve online feature values
- `materialize_features`: Materialize features for a feature view
- `materialize_incremental`: Materialize features incrementally
- `push_features`: Push features to the feature store
- `get_feature_view_schema`: Get the schema of a feature view

## Available Prompts

The MCP server provides the following prompts:

- `feast_feature_store_overview`: Provides an overview of the Feast feature store
- `feast_feature_retrieval_guide`: Provides a guide on how to retrieve features from the feature store
- `feast_feature_exploration`: Provides guidance on how to explore the feature store

## Example

Here's an example of how to use the MCP client to interact with the Feast feature store:

```python
from mcp.client import MCPClient

# Connect to the MCP server
client = MCPClient(url="http://localhost:8000/mcp")

# Get an overview of the feature store
overview = client.get_prompt("feast_feature_store_overview")
print(overview)

# List all feature views
feature_views = client.get_resource("feast://feature-views")
print(feature_views)

# Get details of a specific feature view
driver_stats = client.get_resource("feast://feature-view/driver_stats")
print(driver_stats)

# Retrieve online features
response = client.invoke_tool("get_online_features", {
    "entity_rows": [{"driver_id": 1001}],
    "features": ["driver_stats:conv_rate", "driver_stats:avg_daily_trips"],
    "full_feature_names": False
})
print(response)
```
