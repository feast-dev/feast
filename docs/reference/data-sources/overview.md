# Overview

## Functionality

In Feast, each batch data source is associated with corresponding offline stores.
For example, a `SnowflakeSource` can only be processed by the Snowflake offline store, while a `FileSource` can be processed by both File and DuckDB offline stores.
Otherwise, the primary difference between batch data sources is the set of supported types.
Feast has an internal type system that supports primitive types (`bytes`, `string`, `int32`, `int64`, `float32`, `float64`, `bool`, `timestamp`), array types, set types, map/JSON types, and struct types.
However, not every batch data source supports all of these types.

For more details on the Feast type system, see [here](../type-system.md).

## Per-DataSource Credentials (ConnectionRef)

By default, every data source inherits connection credentials from the global `feature_store.yaml` offline store configuration. The `ConnectionRef` feature allows each data source to declare its own external credential reference, enabling:

- **Multi-tenant deployments** where different feature views access different accounts or databases.
- **Credential isolation** where secrets are resolved at runtime from external providers (Kubernetes Secrets, HashiCorp Vault, environment variables) rather than stored in configuration files.
- **Hybrid offline store** routing where a single Feast deployment connects to multiple backends, each with independent credentials.

### ConnectionRef structure

A `ConnectionRef` is attached to any data source via the `connection_ref` parameter:

```python
from feast.credentials import ConnectionRef
from feast.infra.offline_stores.snowflake_source import SnowflakeSource

source = SnowflakeSource(
    table="USER_FEATURES",
    connection_ref=ConnectionRef(
        provider="kubernetes",
        name="snowflake-creds",
        namespace="ml-team",
        connection_type="snowflake.offline",
        auth_type="secret",
        params={"account": "xy12345", "warehouse": "COMPUTE_WH"},
    ),
)
```

| Field | Description | Required |
|-------|-------------|----------|
| `provider` | Credential backend — `"kubernetes"`, `"vault"`, `"env"`, `"aws-secrets-manager"`, `"gcp-secret-manager"`, `"azure-key-vault"` | Yes |
| `name` | Provider-specific identifier — K8s Secret name, Vault path, env-var prefix, etc. | Yes |
| `namespace` | Scope qualifier — K8s namespace, Vault mount, AWS region, etc. | No |
| `connection_type` | Offline store type (e.g., `"snowflake.offline"`, `"bigquery"`, `"spark"`) | No |
| `auth_type` | Authentication mechanism — `"secret"` (default), `"oauth2"`, `"basic"`, `"sigv4"` | No |
| `params` | Non-sensitive connection parameters (account, database, warehouse, endpoint) | No |

### Credential providers

Feast ships with built-in providers that can be registered at startup:

| Provider | Resolves from | `name` is | `namespace` is |
|----------|---------------|-----------|----------------|
| `env` | Environment variables | Variable prefix | — |
| `kubernetes` | Kubernetes Secrets | Secret name | K8s namespace |
| `vault` | HashiCorp Vault | Secret path | Vault mount |

Custom providers can be registered via:

```python
from feast.credentials import register_credential_provider, CredentialProvider, ConnectionRef

class MyProvider(CredentialProvider):
    def provider_type(self) -> str:
        return "my-provider"

    def resolve(self, ref: ConnectionRef) -> dict:
        # Return key-value credential pairs
        return {"username": "...", "password": "..."}

register_credential_provider(MyProvider())
```

### How it works

1. When an offline store needs to connect, it checks whether the data source has a `connection_ref`.
2. If present, credentials are resolved from the external provider at runtime.
3. Resolved credentials (and any `params` from the `ConnectionRef`) are merged and used to override the global offline store configuration for that specific operation.
4. If no `connection_ref` is set, the data source uses the global `feature_store.yaml` configuration as before.

For usage with the Hybrid Offline Store, see [Hybrid Offline Store](../offline-stores/hybrid.md).

## Functionality Matrix

There are currently four core batch data source implementations: `FileSource`, `BigQuerySource`, `SnowflakeSource`, and `RedshiftSource`.
There are several additional implementations contributed by the Feast community (`PostgreSQLSource`, `SparkSource`, and `TrinoSource`), which are not guaranteed to be stable or to match the functionality of the core implementations.
Details for each specific data source can be found [here](README.md).

Below is a matrix indicating which data sources support which types.

| | File | BigQuery | Snowflake | Redshift | Postgres | Spark | Trino | Couchbase |
| :-------------------------------- | :-- | :-- |:----------| :-- | :-- | :-- | :-- |:----------|
| `bytes`     | yes | yes | yes       | yes | yes | yes | yes | yes |
| `string`    | yes | yes | yes       | yes | yes | yes | yes | yes |
| `int32`     | yes | yes | yes       | yes | yes | yes | yes | yes |
| `int64`     | yes | yes | yes       | yes | yes | yes | yes | yes |
| `float32`   | yes | yes | yes       | yes | yes | yes | yes | yes |
| `float64`   | yes | yes | yes       | yes | yes | yes | yes | yes |
| `bool`      | yes | yes | yes       | yes | yes | yes | yes | yes |
| `timestamp` | yes | yes | yes       | yes | yes | yes | yes | yes |
| array types | yes | yes | yes       | no  | yes | yes | yes | no  |
| `Map`       | yes | no  | yes       | yes | yes | yes | yes | no  |
| `Json`      | yes | yes | yes       | yes | yes | no  | no  | no  |
| `Struct`    | yes | yes | no        | no  | yes | yes | no  | no  |
| set types   | yes* | no | no       | no  | no  | no  | no  | no  |

\* **Set types** are defined in Feast's proto and Python type system but are **not inferred** by any backend. They must be explicitly declared in the feature view schema and are best suited for online serving use cases. See [Type System](../type-system.md#set-types) for details.
