# Remote online store

## Description

This remote online store lets you interact with a remote feature server. You can use this online store to retrieve online features via `store.get_online_features()` from a remote feature server.

## Examples

The registry is pointing to registry of remote feature store. If it is not accessible then should be configured to use remote registry.

{% code title="feature_store.yaml" %}
```yaml
project: my-local-project
registry: /remote/data/registry.db
provider: local
online_store:
  path: http://localhost:6566
  type: remote
  cert: /path/to/cert.pem
entity_key_serialization_version: 3
auth:
  type: no_auth
```
{% endcode %}

`cert` is an optional configuration to the public certificate path when the online server starts in TLS(SSL) mode. This may be needed if the online server is started with a self-signed certificate, typically this file ends with `*.crt`, `*.cer`, or `*.pem`.

## Connection Pooling Configuration

The remote online store uses HTTP connection pooling to improve performance by reusing TCP/TLS connections across multiple requests. This significantly reduces latency by avoiding the overhead of establishing new connections for each request.

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `connection_pool_size` | int | 50 | Maximum number of connections to keep in the pool. Increase for high-concurrency workloads. |
| `connection_idle_timeout` | int | 300 | Maximum time in seconds a session can be idle before being closed. Set to `0` to disable idle timeout. |
| `connection_retries` | int | 3 | Number of retries for failed requests with exponential backoff. |

### Example with Connection Pooling

{% code title="feature_store.yaml" %}
```yaml
project: my-local-project
registry: /remote/data/registry.db
provider: local
online_store:
  type: remote
  path: http://feast-feature-server:80
  
  # Connection pooling configuration (optional)
  connection_pool_size: 50        # Max connections in pool
  connection_idle_timeout: 300    # Idle timeout in seconds (0 to disable)
  connection_retries: 3           # Retry count with exponential backoff
  
entity_key_serialization_version: 3
auth:
  type: no_auth
```
{% endcode %}

### Use Cases

**High-throughput workloads:**
```yaml
online_store:
  type: remote
  path: http://feast-server:80
  connection_pool_size: 100       # More connections for high concurrency
  connection_idle_timeout: 600    # 10 minutes idle timeout
  connection_retries: 5           # More retries for resilience
```

**Long-running services:**
```yaml
online_store:
  type: remote
  path: http://feast-server:80
  connection_idle_timeout: 0      # Never auto-close session
```

**Resource-constrained environments:**
```yaml
online_store:
  type: remote
  path: http://feast-server:80
  connection_pool_size: 10        # Fewer connections to reduce memory
  connection_idle_timeout: 60     # 1 minute timeout
```

### Performance Benefits

Connection pooling provides significant latency improvements:

- **Without pooling**: Each request requires a new TCP connection (~10-50ms) and TLS handshake (~30-100ms)
- **With pooling**: Subsequent requests reuse existing connections, eliminating connection overhead

This is especially beneficial for:
- High-frequency feature retrieval in real-time inference pipelines
- Batch processing with many sequential `get_online_features()` calls
- Services with authentication enabled (reduces token refresh overhead)

### Session Cleanup

The HTTP session is automatically managed with idle timeout, but you can also explicitly close it when your application is shutting down or when you want to release resources.

#### Using FeatureStore context manager (recommended)

The recommended way to ensure proper cleanup is to use the `FeatureStore` as a context manager:

```python
from feast import FeatureStore

# Session is automatically closed when exiting the context
with FeatureStore(repo_path=".") as store:
    features = store.get_online_features(
        features=["driver_hourly_stats:conv_rate"],
        entity_rows=[{"driver_id": 1001}]
    )
```

#### Explicit cleanup

You can also explicitly close the session by calling `close()` on the `FeatureStore`:

```python
from feast import FeatureStore

store = FeatureStore(repo_path=".")
try:
    features = store.get_online_features(
        features=["driver_hourly_stats:conv_rate"],
        entity_rows=[{"driver_id": 1001}]
    )
finally:
    store.close()  # Closes HTTP session and releases resources
```

#### Direct session management

For advanced use cases, you can directly manage the HTTP session via `HttpSessionManager`:

```python
from feast.permissions.client.http_auth_requests_wrapper import HttpSessionManager

# Close the cached HTTP session
HttpSessionManager.close_session()
```

## How to configure Authentication and Authorization
Please refer the [page](./../../../docs/getting-started/concepts/permission.md) for more details on how to configure authentication and authorization.

