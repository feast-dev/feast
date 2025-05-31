 # Feast MCP Feature Server Example

This example demonstrates how to enable MCP (Model Context Protocol) support in Feast, allowing AI agents and applications to interact with your features through standardized MCP interfaces.

## Prerequisites

1. Python 3.8+
2. Feast installed
3. FastAPI MCP library

## Installation

1. Install Feast with MCP support:
```bash
pip install feast[mcp]
```

Alternatively, you can install the dependencies separately:
```bash
pip install feast
pip install fastapi_mcp
```

## Setup

1. Navigate to this example directory within your cloned Feast repository:
```bash
cd examples/mcp_feature_store
```

2. Initialize a Feast repository in this directory. We'll use the existing `feature_store.yaml` that's already configured for MCP:
```bash
feast init . 
```
This will create a `data` subdirectory and a `feature_repo` subdirectory if they don't exist, and will use the `feature_store.yaml` present in the current directory (`examples/mcp_feature_store`).

3. Apply the feature store configuration:
```bash
cd feature_repo 
feast apply
cd .. # Go back to examples/mcp_feature_store for the next steps
```

## Starting the MCP-Enabled Feature Server

Start the Feast feature server with MCP support:

```bash
feast serve --host 0.0.0.0 --port 6566
```

If MCP is properly configured, you should see a log message indicating that MCP support has been enabled:

```
INFO:feast.feature_server:MCP support has been enabled for the Feast feature server
```

## Available MCP Tools

The fastapi_mcp integration automatically exposes your Feast feature server's FastAPI endpoints as MCP tools. This means AI assistants can:

- **Call `/get-online-features`** to retrieve features from the feature store
- **Use `/health`** to check server status  


## Configuration Details

The key configuration that enables MCP support:

```yaml
feature_server:
    type: mcp                    # Use MCP feature server type
    enabled: true               # Enable feature server
    mcp_enabled: true           # Enable MCP protocol support
    mcp_server_name: "feast-feature-store"
    mcp_server_version: "1.0.0"
```