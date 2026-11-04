# Feast standalone MCP server example

This guide explains how to run the standalone Feast MCP server (`feast mcp`) against a local feature store, and how to call its tools from an MCP client.

Unlike the [MCP feature store example](../mcp_feature_store/README.md), which enables MCP inside the feature server process, `feast mcp` runs as a separate server. It holds no registry or online store of its own, and instead proxies to a running Feast deployment, exposing two tool namespaces:

| Namespace | Proxies | Mounted when |
| --------- | ------- | ------------ |
| `features_*` | The feature server (`feast serve`) | `features.url` is set |
| `registry_*` | The REST registry server (`feast serve_registry --rest-api`) | `registry.url` is set |

See the [Standalone MCP server](../../docs/reference/feature-servers/mcp-server.md) reference for the full list of options.

## Files

- [feast_mcp.yaml](feast_mcp.yaml): MCP server configuration, covering transport, upstream URLs, authentication, and logging.
- [mcp_client_demo.py](mcp_client_demo.py): A minimal MCP client that lists the available tools and calls one from each namespace.
- [kubernetes/featurestore-mcpserver.yaml](kubernetes/featurestore-mcpserver.yaml): The same deployment on Kubernetes, using the Feast Operator.

## Prerequisites

1. **Python 3.10+ environment**
2. **Feast with the MCP server extra**: `pip install 'feast[mcp-server]'`.

## Setup

### 1. **Create a feature repository**

The MCP server needs a running Feast deployment to proxy to, so start by creating a local feature store. From this directory:

```bash
feast init -t local feast_demo
```

- Apply the feature definitions to register the `driver_hourly_stats` feature view that the demo client queries:

  ```bash
  cd feast_demo/feature_repo && feast apply && cd ../..
  ```

- Load the online store, so that the feature retrieval returns actual values rather than nulls:

  ```bash
  cd feast_demo/feature_repo && feast materialize-incremental "$(date -u +%Y-%m-%dT%H:%M:%S)" && cd ../..
  ```

> **Note:** `feast init` names the project after the directory, so the project here is `feast_demo`, not `my_project`.

### 2. **Start the Feast servers**

Both upstream servers run from the feature repository directory. In two separate terminals, from `feast_demo/feature_repo`:

- Start the feature server:

  ```bash
  feast serve --host 0.0.0.0 --port 6566
  ```

- Start the registry server. The registry serves gRPC by default, so `--rest-api` is required here because the MCP server talks to the registry over REST:

  ```bash
  feast serve_registry --rest-api --rest-port 6572
  ```

### 3. **Start the MCP server**

In a third terminal, from this directory:

```bash
feast mcp --config feast_mcp.yaml
```

- The provided [feast_mcp.yaml](feast_mcp.yaml) sets `transport: http`, so the MCP endpoint is served at `http://localhost:8000/mcp`. The same configuration can be passed entirely on the command line:

  ```bash
  feast mcp --feast-url http://localhost:6566 --registry-url http://localhost:6572 --transport http --port 8000
  ```

- Verify that the server is running:

  ```bash
  curl -s http://localhost:8000/health
  ```

  Example output:

  ```
  {"status":"healthy","service":"mcp-server"}
  ```

> **Note:** Settings are resolved in the order CLI options, environment variables, config file, defaults. Because environment variables outrank the file, an exported `FEAST_MCP_TRANSPORT` will override `server.transport` in `feast_mcp.yaml`.

### 4. **Call the tools from a client**

The [mcp_client_demo.py](mcp_client_demo.py) script connects over HTTP, lists the tools that the server mounted, and then calls one tool from each namespace:

```bash
python mcp_client_demo.py
```

Example output:

```
22 tools mounted:
  - features_get_online_features
  - features_get_vector_store
  - features_health
  - features_list_vector_stores
  - features_materialize
  - features_materialize_incremental
  - features_push
  - features_search
  - features_vector_store_search
  - registry_get_data_source
  - registry_get_entity
  - registry_get_feature_service
  - registry_get_feature_view
  - registry_get_lineage
  - registry_get_project
  - registry_list_data_sources
  - registry_list_entities
  - registry_list_feature_services
  - registry_list_feature_views
  - registry_list_features
  - registry_list_projects
  - registry_search_registry

registry_list_projects: ['feast_demo']
registry_list_feature_views (project=feast_demo): ['driver_hourly_stats', 'driver_hourly_stats_fresh', 'transformed_conv_rate', 'transformed_conv_rate_fresh']

features_get_online_features:
{'results': [{'values': [1001, 1002], 'statuses': ['PRESENT', 'PRESENT'], ...}, ...], 'metadata': {'feature_names': ['driver_id', 'acc_rate', 'conv_rate']}}
```

- The `PRESENT` statuses confirm that the materialization step in stage 1 succeeded. If it is skipped, the statuses come back as `NOT_FOUND` with null values, while the registry tools continue to work.
- If only one namespace appears, the other upstream URL was not configured. Each sub-server is mounted only when its URL is set.

### 5. **Connect an IDE client**

For an HTTP transport, point the MCP client at `http://localhost:8000/mcp`.

For a stdio transport, let the client spawn the process instead. Since `feast mcp` reads `feast_mcp.yaml` from the working directory, pass `--config` with an absolute path:

```json
{
  "mcpServers": {
    "feast": {
      "command": "feast",
      "args": ["mcp", "--config", "/absolute/path/to/feast_mcp.yaml", "--transport", "stdio"]
    }
  }
}
```

### 6. **Cleanup**

- Stop the three servers, then remove the generated feature repository:

  ```bash
  rm -rf feast_demo
  ```

## Authentication

This example runs with `auth.mode: passthrough`, the default, which accepts connections without a token. The MCP server does not enforce its own RBAC. A token supplied by the client is forwarded to Feast, which applies its own [permission model](../../docs/getting-started/concepts/permission.md).

To give IDE clients a browser login flow, switch to OIDC. This is usually configured against the same provider already set as `auth.oidc_discovery_url` in `feature_store.yaml`:

```bash
feast mcp --config feast_mcp.yaml --auth-mode oidc --oidc-discovery-url https://keycloak.example.com/realms/feast/.well-known/openid-configuration --oidc-client-id feast-mcp --base-url http://localhost:8000
```

> **Note:** Run a single replica with `oidc`. The OAuth state store is per-node and on disk, so a callback routed to a replica that did not handle the authorize request will fail.

### Kubernetes authentication

This example uses `--auth-mode passthrough`: the client sends its Service Account or user token as a bearer token, the MCP server forwards it unchanged, and the upstream Feast servers validate it using the TokenReview API. For an in-cluster MCP deployment, `--auth-mode kubernetes` also validates the token at the MCP server before forwarding it. See the [Kubernetes authentication requirements](../../docs/reference/feature-servers/mcp-server.md#kubernetes-authentication) for the required dependencies and Service Account permissions.

The demo client sends whatever is in the `MCP_TOKEN` environment variable as its bearer token:

```bash
MCP_TOKEN=$(kubectl create token my-service-account) python mcp_client_demo.py
```

This matters when running on a cluster, as described in the next section.

## Running on Kubernetes

The [kubernetes/featurestore-mcpserver.yaml](kubernetes/featurestore-mcpserver.yaml) manifest deploys the same setup with the [Feast Operator](../../infra/feast-operator/README.md). Setting `spec.services.mcpServer` adds a `feast mcp` container to the FeatureStore deployment, exposed on its own Service on port 8100.

The Feast Operator enables Kubernetes authentication by default, so the online store and registry in this manifest both require a bearer token. Because the MCP server only relays the caller's token and never supplies one of its own, a client that connects without a token will see `401 Unauthorized` errors raised from upstream.

- Apply the manifest:

  ```bash
  kubectl apply -f kubernetes/featurestore-mcpserver.yaml
  ```

- Wait for the MCP server to become ready:

  ```bash
  kubectl wait --for=condition=McpServer featurestore/sample-mcpserver --timeout=300s
  ```

- Forward the Service port and run the same demo client against it:

  ```bash
  kubectl port-forward svc/feast-sample-mcpserver-mcpserver 8000:8100
  ```

  ```bash
  python mcp_client_demo.py
  ```

  Since Kubernetes auth is on, pass a token so that the demo client can reach the upstream servers:

  ```bash
  MCP_TOKEN=$(kubectl create token default) python mcp_client_demo.py
  ```

  Alternatively, for a development cluster, set `spec.authz.noAuth: true` on the FeatureStore to disable authentication entirely.

- To remove the deployment:

  ```bash
  kubectl delete -f kubernetes/featurestore-mcpserver.yaml
  ```

Two operator-specific behaviors are worth noting:

- The operator sets `--host` and `--port` so that they match the generated Service. The `server.transport` value in the ConfigMap is still honored, but `server.host` and `server.port` are not.
- A CEL validation rule enforces that at least one upstream is available: either `onlineStore` is not disabled (omitting it is fine — the operator defaults an online feature server), or `registry.local.server.restAPI` is `true`.
