# Snowflake

## Description

The [Snowflake](https://trial.snowflake.com) batch materialization engine provides a highly scalable and parallel execution engine using a Snowflake Warehouse for batch materializations operations (`materialize` and `materialize-incremental`) when using a `SnowflakeSource`.

The engine requires no additional configuration other than for you to supply Snowflake's standard login and context details. The engine leverages custom (automatically deployed for you) Python UDFs to do the proper serialization of your offline store data to your online serving tables.

When using all three options together, `snowflake.offline`, `snowflake.engine`, and `snowflake.online`, you get the most unique experience of unlimited scale and performance + governance and data security.

## Example

{% code title="feature_store.yaml" %}
```yaml
...
offline_store:
  type: snowflake.offline
...
batch_engine:
  type: snowflake.engine
  account: snowflake_deployment.us-east-1
  user: user_login
  password: user_password
  role: sysadmin
  warehouse: demo_wh
  database: FEAST
```
{% endcode %}
