# Snowflake

## Description

The [Snowflake](https://trial.snowflake.com) online store provides support for materializing feature values into a Snowflake Transient Table for serving online features.

* Only the latest feature values are persisted

The data model for using a Snowflake Transient Table as an online store follows a tall format (one row per feature)):
* "entity_feature_key" (BINARY) -- unique key used when reading specific feature_view x entity combination
* "entity_key" (BINARY) -- repeated key currently unused for reading entity_combination
* "feature_name" (VARCHAR)
* "value" (BINARY)
* "event_ts" (TIMESTAMP)
* "created_ts" (TIMESTAMP)

 (This model may be subject to change when Snowflake Hybrid Tables are released)

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
online_store:
    type: snowflake.online
    account: SNOWFLAKE_DEPLOYMENT_URL
    user: SNOWFLAKE_USER
    password: SNOWFLAKE_PASSWORD
    role: SNOWFLAKE_ROLE
    warehouse: SNOWFLAKE_WAREHOUSE
    database: SNOWFLAKE_DATABASE
```
{% endcode %}
