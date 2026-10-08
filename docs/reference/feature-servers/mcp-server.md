# Standalone MCP server

## Overview

The standalone MCP server runs the Model Context Protocol in its own process, separate from the feature server. It is started with `feast mcp` and holds no registry or online store of its own. Instead, it proxies to a running Feast deployment and exposes its HTTP APIs as MCP tools, so that AI agents and MCP-capable clients can retrieve features and browse the registry.

The server has two sub-servers. Each one is mounted only when you set the URL of the Feast server it talks to:

| Sub-server | Proxies | Mounted when |
| ---------- | ------- | ------------ |
| `features` | The Python feature server (`feast serve`) | `--feast-url` or `features.url` is set |
| `registry` | The REST registry server (`feast serve_registry --rest-api`) | `--registry-url` or `registry.url` is set |

At least one of the two must be configured, otherwise the server exits with a usage error.

This is not the same as the [MCP Feature Server](mcp-feature-server.md), which sets `mcp_enabled: true` in `feature_store.yaml` to mount an OpenAPI-derived MCP endpoint inside the feature server process. The standalone server is a separate deployable that can front both the feature server and the registry at once, with its own tools, authentication mode, and logging settings. The two can be used together.

## Installation

```bash
pip install 'feast[mcp-server]'
```

The `minimal` extra pulls in `mcp-server`, so the published `feature-server` image already includes `feast mcp`.

## CLI

There is a CLI command that starts the server: `feast mcp`.

```bash
feast mcp --feast-url http://localhost:6566 --registry-url http://localhost:6572 --transport http --port 8000
```

A `feast-mcp` console script is also installed. It is equivalent to `feast mcp`, but skips loading the rest of the Feast CLI.

**Connection options:**
* `--config`: Path to the `feast_mcp.yaml` config file
* `--feast-url`: URL of the feature server to proxy. Mounts the `features` tools
* `--registry-url`: URL of the REST registry server to proxy. Mounts the `registry` tools
* `--timeout`: HTTP timeout in seconds for calls to Feast (default: 30)

**Server options:**
* `--transport`: MCP transport to serve: `stdio`, `http`, `streamable-http`, or `sse` (default: `stdio`)
* `--host`: Bind address for HTTP transports (default: `0.0.0.0`)
* `--port`: Bind port for HTTP transports (default: 8000)
* `--workers`: Run under gunicorn with this many workers. Not supported by the `sse` transport

**Authentication options:**
* `--auth-mode`: `passthrough`, `kubernetes` or `oidc` (default: `passthrough`)
* `--oidc-discovery-url`: OIDC discovery document URL. Required with `--auth-mode oidc`
* `--oidc-client-id`: OIDC client id. Required with `--auth-mode oidc`
* `--oidc-client-secret`: OIDC client secret
* `--oidc-audience`: Expected OIDC token audience
* `--base-url`: Public base URL of this server, used to build OAuth redirect URIs (default: `http://localhost:<port>`)

**Logging options:**
* `--log-level`: Log level (default: `INFO`)
* `--log-format`: Console log format, `text` or `json` (default: `text`)

## Endpoints

For the HTTP transports, MCP is mounted at:

- `/mcp` for `http` and `streamable-http`
- `/sse` for `sse`

The server also exposes an unauthenticated health endpoint at `GET /health`.

## Configuration

Settings are resolved in priority order: CLI options, then environment variables, then the config file, then defaults. Note that environment variables outrank the config file, so a `FEAST_MCP_*` variable set in the environment will override the same setting in `feast_mcp.yaml`.

If `--config` is not passed, `feast_mcp.yaml` (then `feast_mcp.yml`) is read from the current working directory when present.

```yaml
server:
  transport: http           # stdio | http | streamable-http | sse
  host: 0.0.0.0
  port: 8000
  # workers: 4              # gunicorn; not supported with sse

# At least one of features.url / registry.url is required.
features:
  url: http://localhost:6566
registry:
  url: http://localhost:6572

timeout: 30

observability:
  level: INFO               # DEBUG | INFO | WARNING | ERROR
  format: json              # text | json
  # stdio: true             # also log to stderr (default true)

# auth:
#   mode: oidc              # passthrough | kubernetes | oidc
#   discovery_url: https://keycloak.example.com/realms/feast/.well-known/openid-configuration
#   client_id: feast-mcp
#   client_secret: null
#   audience: null
#   base_url: https://mcp.example.com
```

### Environment variables

| Variable | Maps to |
| -------- | ------- |
| `FEAST_MCP_FEATURE_SERVER_URL` | `features.url` |
| `FEAST_MCP_REGISTRY_URL` | `registry.url` |
| `FEAST_MCP_TRANSPORT` | `server.transport` |
| `FEAST_MCP_WORKERS` | `server.workers` |
| `FEAST_MCP_TIMEOUT` | `timeout` |
| `FEAST_MCP_AUTH_MODE` | `auth.mode` |
| `FEAST_MCP_OIDC_DISCOVERY_URL` | `auth.discovery_url` |
| `FEAST_MCP_OIDC_CLIENT_ID` | `auth.client_id` |
| `FEAST_MCP_OIDC_CLIENT_SECRET` | `auth.client_secret` |
| `FEAST_MCP_OIDC_AUDIENCE` | `auth.audience` |
| `FEAST_MCP_BASE_URL` | `auth.base_url` |
| `FEAST_MCP_LOG_LEVEL` | `observability.level` |
| `FEAST_MCP_LOG_FORMAT` | `observability.format` |
| `FEAST_MCP_LOG_STDIO` | `observability.stdio` |

> **Note:** `server.host` and `server.port` have no environment equivalent. They can only be set on the command line or in the config file.

## Available tools

Tools are namespaced by the sub-server that provides them, for example `features_get_online_features` and `registry_list_projects`.

**Tools provided by the `features` sub-server:**
* `get_online_features`: Retrieve online feature values for a set of entities
* `search`: Vector similarity search against online document embeddings
* `list_vector_stores`, `get_vector_store`: List and inspect available vector stores
* `vector_store_search`: OpenAI-compatible vector store search
* `push`: Push features into the online or offline store
* `materialize`, `materialize_incremental`: Materialize features from the offline store to the online store
* `health`: Check the health of the Feast feature server

**Tools provided by the `registry` sub-server:**
* `list_projects`, `get_project`: Browse projects
* `list_entities`, `get_entity`: Browse entities
* `list_feature_views`, `get_feature_view`: List and inspect feature views
* `list_features`: List individual features (columns) across all feature views
* `list_feature_services`, `get_feature_service`: Browse feature services
* `list_data_sources`, `get_data_source`: Browse data sources
* `search_registry`: Full-text search across all registry objects
* `get_lineage`: Retrieve lineage relationships between registry objects

HTTP errors from Feast are raised as MCP tool errors rather than returned as tool results, so a `403` from Feast's permission model reaches the client as a failure instead of being passed to the model as feature data.

## Authentication

The MCP server checks *who* the caller is. It does not decide *what* they are allowed to do. The caller's token is always sent on to the Feast servers behind it, and those servers check the token again and apply their own [permission model](../../getting-started/concepts/permission.md). This means Feast sees the real user, not a service account of the MCP server.

There are three modes. They use the same names as the `auth.type` values in `feature_store.yaml`:

* **`passthrough`** (default): nothing is checked here. A client can connect without a token. If the client does send a token, it is passed on as-is. Use this for local development, or when the MCP server runs outside the cluster and cannot check tokens itself.
* **`kubernetes`**: Service Account and user tokens are checked with the Token Access Review API before any tool runs. See [Kubernetes authentication](#kubernetes-authentication) below.
* **`oidc`**: the server fronts an OIDC provider so that IDE clients such as Cursor and VS Code can complete a browser login flow. The resulting access token is forwarded on every tool call. Programmatic clients can also send OIDC provider tokens directly as bearer tokens, which are validated against the provider's JWKS. This mode requires `--oidc-discovery-url` and `--oidc-client-id`, typically the same values already configured as `auth.oidc_discovery_url` in `feature_store.yaml`.

Mode names are case-sensitive. The server refuses to start with an unknown mode, whether it comes from `--auth-mode`, `FEAST_MCP_AUTH_MODE` or `auth.mode` in YAML, so a typo can never switch authentication off.

> **Note:** `oidc` mode assumes a single replica. The OAuth state store is FastMCP's default, which is per-node and on disk, so a callback routed to a different replica than the authorize request will fail. Run one replica, or use client affinity, until a shared state backend is supported.

### Kubernetes authentication

`--auth-mode kubernetes` uses the same `KubernetesTokenParser` that the Feast feature server and registry server use for `auth.type: kubernetes`. So the MCP server accepts exactly the same tokens they do.

Here is what happens on each request:

1. The client sends its Kubernetes Service Account or user token as a bearer token.
2. The MCP server checks the token with the Token Access Review API.
3. It reads the caller's `Role`s from the RoleBindings in its own namespace.
4. It sends the same token on to Feast.
5. Feast checks the token again and applies its [permission model](../auth/kubernetes_auth_setup.md).

```bash
feast mcp --transport http --feast-url http://feast-feature-server:80 --auth-mode kubernetes
```

```bash
curl -H "Authorization: Bearer $(kubectl create token my-service-account)" http://localhost:8000/mcp
```

Checking the token here does not move any permission decision to the MCP server. It only means a bad token gets a `401` straight away, instead of being passed on to Feast. What a valid user is allowed to do is still decided by Feast.

This mode has three requirements:

* The server must run inside a cluster. It reads the in-cluster config at startup and stops with a clear error if it cannot.
* The Kubernetes client must be installed. It is an optional dependency (`feast[k8s]`), and the feature server image that the MCP image is built from already has it.
* The pod's Service Account needs permission to create `tokenreviews` in the `authentication.k8s.io` API group, and to read RoleBindings in its namespace. The Feast Operator already grants this to the shared Feast Service Account when `spec.authz.kubernetes` is set.

If the MCP server runs outside the cluster, use `passthrough` instead. Feast still checks the token it receives, so callers are still authenticated. The check just happens one hop later.

> **Note:** The MCP container never injects its own pod Service Account token, and the operator does not add one for it. Only a token supplied by the MCP client is forwarded to Feast. In `passthrough` mode the MCP layer itself accepts connections without a token; whether upstream Feast then requires one depends on Feast's own auth settings.

> **Note:** The Feast Operator turns on Kubernetes authentication by default for all deployed services. So an MCP client that connects without a token gets `401 Unauthorized` from upstream. In `kubernetes` mode the error comes from the MCP server, and in `passthrough` mode it comes from the feature server or the registry. Either send a token, or set `spec.authz.noAuth: true` on the FeatureStore for development.

## Running with Docker

The image entrypoint is the server itself, and its default `CMD` is `--config /config/feast_mcp.yaml`:

```bash
docker buildx build -f sdk/python/feast/mcp/docker/Dockerfile \
  --build-arg BASE_TAG=0.66.0 -t feast-mcp:0.66.0 --load .
```

```bash
docker run --rm -p 8000:8000 -v "$PWD/feast_mcp.yaml:/config/feast_mcp.yaml:ro" feast-mcp:0.66.0
```

Passing arguments replaces the default `CMD`:

```bash
docker run --rm -p 8000:8000 feast-mcp:0.66.0 --feast-url http://host.docker.internal:6566 --transport http
```

See [sdk/python/feast/mcp/docker/README.md](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/mcp/docker/README.md) for the build arguments and for deploying with the mcp-lifecycle-operator.

## Deploying with the Feast Operator

Setting `spec.services.mcpServer` adds a dedicated `feast mcp` container to the FeatureStore deployment, exposed on its own Service on port 8100. The operator sets `--host` and `--port` so that they match the generated Service. Every other setting comes from a `feast_mcp.yaml` supplied in a ConfigMap, which the operator mounts read-only at `/etc/feast/mcp`.

The ConfigMap must set `server.transport` to `http`, `streamable-http` or `sse`, because the default `stdio` cannot serve the Service. Without a ConfigMap, the operator passes `--transport http`, unless `FEAST_MCP_TRANSPORT` is set in the container `env`.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: sample-mcpserver-config
data:
  feast_mcp.yaml: |
    server:
      # host and port are set by the operator and override this file.
      # transport is honored from here.
      transport: http
    # The MCP container runs in the same pod as the online and registry
    # servers, so it reaches them over localhost.
    features:
      url: http://localhost:6566
    registry:
      url: http://localhost:6572
    timeout: 30
    observability:
      level: INFO
      format: json
---
apiVersion: feast.dev/v1
kind: FeatureStore
metadata:
  name: sample-mcpserver
spec:
  feastProject: my_project
  services:
    onlineStore:
      server: {}
    registry:
      local:
        server:
          restAPI: true
    mcpServer:
      config:
        configMapRef:
          name: sample-mcpserver-config
```

| Field | Type | Default | Description |
| ----- | ---- | ------- | ----------- |
| `config.configMapRef.name` | string | — | ConfigMap in the same namespace holding the MCP config |
| `config.configMapKey` | string | `feast_mcp.yaml` | Key in the ConfigMap holding the config content |

`mcpServer` also accepts the standard container settings shared by the other servers: `image`, `env`, `envFrom`, `imagePullPolicy`, `resources`, `nodeSelector`, and `logLevel`. When `image` is omitted, the operator uses the shared `feature-server` image (which already includes `feast mcp`). Readiness is reported on the `McpServer` status condition (probed at `GET /health`), and the generated Service hostname on `status.serviceHostnames.mcpServer`.

A CEL validation rule enforces that at least one Feast service is available: either `onlineStore` is not disabled (omitting it is fine — the operator defaults an online feature server), or `registry.local.server.restAPI` is `true`.

> **Note:** Operator-managed TLS is not yet supported for the MCP server. The `tls` field is ignored. The embedded `metrics` and `workerConfigs` fields are also ignored for `mcpServer`; set `server.workers` in the ConfigMap if you need multiple gunicorn workers.

## Connecting an MCP client

For an HTTP transport, point the client at the MCP endpoint. For example, if the server runs at `http://localhost:8000`, use:

- `http://localhost:8000/mcp`

For a stdio client, let the client spawn the process:

```json
{
  "mcpServers": {
    "feast": {
      "command": "feast",
      "args": ["mcp", "--feast-url", "http://localhost:6566", "--registry-url", "http://localhost:6572"]
    }
  }
}
```

## Example

See [examples/feast_mcp_server](https://github.com/feast-dev/feast/tree/master/examples/feast_mcp_server) for an end-to-end walkthrough that starts a local feature store, runs the MCP server against it, and calls its tools from a client.

## Troubleshooting

- If you see `The standalone MCP server could not be imported`, the `mcp-server` extra is not installed. Install it with `pip install 'feast[mcp-server]'`.
- If the server exits with `At least one of --feast-url or --registry-url must be provided`, no Feast URL was resolved from the CLI, the environment, or the config file. If you expected the file to supply it, check that you are running from the directory that holds `feast_mcp.yaml`, or pass `--config` explicitly.
- If a setting in `feast_mcp.yaml` appears to be ignored, check for a `FEAST_MCP_*` environment variable, which takes precedence over the file.
- If the server rejects `--workers` with `SSE transport does not support multiple workers`, switch to `--transport http` or `streamable-http`, or omit `--workers`.
- If a tool namespace is missing, its Feast URL was not configured. The `features_*` tools require `--feast-url`, and the `registry_*` tools require `--registry-url` together with a registry server started using `--rest-api`.
