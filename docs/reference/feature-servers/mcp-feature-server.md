# MCP Feature Server

## Overview

Feast can expose the Python Feature Server as an MCP (Model Context Protocol) server using `fastapi_mcp`. When enabled, MCP clients can discover and call Feast tools such as online feature retrieval.

## Installation

```bash
pip install feast[mcp]
```

## Configuration

Add an MCP `feature_server` block to your `feature_store.yaml`:

```yaml
feature_server:
  type: mcp
  enabled: true
  mcp_enabled: true
  mcp_transport: http
  mcp_server_name: "feast-feature-store"
  mcp_server_version: "1.0.0"
```

### mcp_transport

`mcp_transport` controls how MCP is mounted into the Feature Server:

- `sse`: SSE-based transport. This is the default for backward compatibility.
- `http`: Streamable HTTP transport. This is recommended for improved compatibility with some MCP clients.

If `mcp_transport: http` is configured but your installed `fastapi_mcp` version does not support Streamable HTTP mounting, Feast will fail fast with an error asking you to upgrade `fastapi_mcp` (or reinstall `feast[mcp]`).

## Endpoints

MCP is mounted at:

- `/mcp`

## Connecting an MCP client

Use your MCP client’s “HTTP” configuration and point it to the Feature Server base URL. For example, if your Feature Server runs at `http://localhost:6566`, use:

- `http://localhost:6566/mcp`

## Troubleshooting

- If you see a deprecation warning about `mount()` at runtime, upgrade `fastapi_mcp` and use `mcp_transport: http` or `mcp_transport: sse`.
- If your MCP client has intermittent connectivity issues with `mcp_transport: sse`, switch to `mcp_transport: http`.
