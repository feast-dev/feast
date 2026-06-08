# Feast MLflow Tracing

## Configuration

Add to `feature_store.yaml`:

```yaml
mlflow:
  enabled: true
  tracking_uri: "http://localhost:5000"
  enable_tracing: true
```

Start MLflow:
```bash
mlflow server --host 0.0.0.0 --port 5000
```

All feature server endpoints will emit MLflow traces automatically.

---

## Automatic Server Traces (MCP & HTTP Clients)

When any client (Cursor, Claude Desktop, or direct HTTP) calls the Feast feature server, traces are created automatically with no client-side code changes.

Each trace includes:
- `feast.feature_refs` — which features were served
- `feast.entity_count` — number of entities in the request
- `feast.project` — project name
- `feast.retrieval_type` — `online`
- `feast.mcp_session_id` — MCP session identifier (when called via MCP)

The `mcp_session_id` groups all tool calls from a single MCP session (e.g., one Cursor chat conversation), allowing you to filter and correlate all feature retrievals made during that session.

**MCP client configuration** (`mcp.json`):
```json
{
  "mcpServers": {
    "feast": {
      "url": "http://127.0.0.1:6567/mcp"
    }
  }
}
```

**Result**: Independent traces per request in MLflow, filterable by `feast.mcp_session_id`.

---

## Cross-Process Trace Linking (HTTP Agents)

When your agent calls the Feast feature server over HTTP, pass the `traceparent` header to produce a unified trace tree in MLflow.

```python
import mlflow
import requests

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.openai.autolog()

@mlflow.trace
def my_agent(entity_id: int):
    headers = mlflow.tracing.get_tracing_context_headers_for_http_request()

    resp = requests.post(
        "http://feast-server:6567/get-online-features",
        json={"features": [...], "entities": {...}},
        headers=headers,
    )
    features = resp.json()

    response = llm.chat.completions.create(...)
    return response.choices[0].message.content
```

**Result**: A single MLflow trace containing both agent spans and Feast server spans.

---

## In-Process Feature Context Tagging (SDK Usage)

When using Feast as a Python library, use `feast_trace_scope` to capture which features were retrieved and attach that metadata to the LLM span.

```python
import mlflow
from feast import FeatureStore
from feast.tracing_context import feast_trace_scope

mlflow.set_tracking_uri("http://localhost:5000")
mlflow.openai.autolog()

store = FeatureStore(repo_path=".")

@mlflow.trace
def my_agent(entity_id: int):
    feature_refs = ["my_view:feature_a", "my_view:feature_b"]

    with feast_trace_scope() as ctx:
        features = store.get_online_features(
            features=feature_refs,
            entity_rows=[{"entity_id": entity_id}],
        )
        ctx.push_retrieval(feature_refs)
        context_attrs = ctx.get_context_attributes()  # must be inside the scope

    with mlflow.start_span(name="llm_call") as span:
        span.set_attributes(context_attrs)
        response = llm.chat.completions.create(...)

    return response.choices[0].message.content
```

**Result**: The LLM span in MLflow contains:
- `feast.context_features` — which features were retrieved
- `feast.context_feature_views` — which feature views were queried
- `feast.context_feature_count` — number of features

---

## API Reference

| Function | Description |
|----------|-------------|
| `mlflow.tracing.get_tracing_context_headers_for_http_request()` | Returns `dict` with `traceparent` header for cross-process linking |
| `feast_trace_scope()` | Context manager that creates a `FeastTraceContext` (clears on exit) |
| `ctx.push_retrieval(feature_refs)` | Records retrieved feature references |
| `ctx.get_context_attributes()` | Returns span-ready `dict` of accumulated metadata |

**Important**: Call `ctx.get_context_attributes()` inside the `with feast_trace_scope()` block. The context is cleared on scope exit.
