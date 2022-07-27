# Snowflake

## Description

The [Snowflake](https://trial.snowflake.com) online store provides support for materializing feature values into an Snowflake Transient Table for serving online features.

* Only the latest feature values are persisted

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
