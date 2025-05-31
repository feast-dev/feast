 # Feast MCP Feature Server Example

This example demonstrates how to enable MCP (Model Context Protocol) support in Feast, allowing AI agents and applications to interact with your feature store through standardized MCP interfaces.

## Prerequisites

1. Python 3.8+
2. Feast installed
3. FastAPI MCP library

## Installation

1. Install Feast if you haven't already:
```bash
pip install feast
```

2. Install the MCP dependencies:
```bash
pip install fastapi_mcp
```

## Setup

1. Initialize the Feast repository:
```bash
feast init feast_mcp_example
cd feast_mcp_example
```

2. Replace the default `feature_store.yaml` with the MCP-enabled configuration:
```bash
cp ../feature_store.yaml .
```

3. Apply the feature store configuration:
```bash
feast apply
```

4. Materialize some sample data (optional):
```bash
feast materialize-incremental $(date +%Y-%m-%d)
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

The server exposes the following MCP tools that can be used by AI agents:

- `get_online_features`: Retrieve feature values for entities
- `list_feature_views`: List all available feature views
- `list_feature_services`: List all available feature services  
- `get_feature_store_info`: Get information about the feature store

## Available MCP Resources

- `feast://feature-views`: JSON resource with all feature views
- `feast://feature-services`: JSON resource with all feature services

## Testing MCP Functionality

You can test the MCP functionality by connecting an MCP-compatible client to the server endpoint. The exact method depends on your MCP client.

## Example MCP Client Interaction

```python
# Example of how an MCP client might interact with the server
# (This would be implemented by your MCP client library)

# Get feature store information
store_info = await mcp_client.call_tool("get_feature_store_info")
print(f"Feature store: {store_info}")

# List available feature views
feature_views = await mcp_client.call_tool("list_feature_views") 
print(f"Available feature views: {feature_views}")

# Get features for specific entities
features = await mcp_client.call_tool("get_online_features", {
    "entities": {"driver_id": [1001, 1002]},
    "features": ["driver_hourly_stats:conv_rate", "driver_hourly_stats:acc_rate"]
})
print(f"Features: {features}")
```

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

## Troubleshooting

### MCP not working
- Ensure `fastapi_mcp` is installed: `pip list | grep fastapi_mcp`
- Check that `mcp_enabled: true` in your `feature_store.yaml`
- Look for error messages in the server logs

### Server starts but no MCP support
- Verify the feature server type is set to "mcp"
- Check that both `enabled: true` and `mcp_enabled: true` are set
- Make sure the fastapi_mcp library is properly installed

### Dependencies missing
If you see warnings about missing dependencies:
```bash
pip install fastapi_mcp
```

Then restart the server.

## Next Steps

1. Integrate with your preferred MCP client
2. Customize the MCP server configuration for your use case
3. Add authentication and security as needed for production deployment
4. Explore additional MCP tools and resources as they become available

## Security Note

In production environments, ensure proper authentication and authorization are configured for your Feast deployment when exposing MCP endpoints. 