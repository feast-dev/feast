# Snowflake Registry

## Description

The [Snowflake](https://trial.snowflake.com) registry provides support for storing the protobuf representation of your feature store objects (data sources, feature views, feature services, etc.) Because Snowflake is an ACID compliant database, this allows for changes to individual objects atomically.

An example of how to configure this would be:

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
provider: local
registry:
  registry_type: snowflake.registry
  account: snowflake_deployment.us-east-1
  user: user_login
  password: user_password
  role: SYSADMIN
  warehouse: COMPUTE_WH
  database: FEAST
  schema: PUBLIC
  cache_ttl_seconds: 60
offline_store:
  ...
```
{% endcode %}

The full set of configuration options is available in [SnowflakeRegistryConfig](https://rtd.feast.dev/en/latest/#feast.infra.registry.snowflake.SnowflakeRegistryConfig).
