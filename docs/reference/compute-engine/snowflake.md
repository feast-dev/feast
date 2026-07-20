# Snowflake

## Description

The [Snowflake](https://trial.snowflake.com) compute engine provides a highly scalable and parallel execution engine using a Snowflake Warehouse for batch materializations operations (`materialize` and `materialize-incremental`) when using a `SnowflakeSource`.

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
  python_udf_runtime_version: "3.10"
```
{% endcode %}

## Configuration

* `python_udf_runtime_version` *(optional, default: `"3.10"`)* -- The Snowflake Python UDF `RUNTIME_VERSION` used when Feast deploys its materialization UDFs. Snowflake periodically decommissions old Python UDF runtimes (for example, the 3.9 runtime was decommissioned, requiring Feast to bump its default to 3.10 -- see [#6606](https://github.com/feast-dev/feast/issues/6606)). If Snowflake decommissions the 3.10 runtime in the future, set this field to a still-supported version (e.g. `"3.11"`) instead of waiting for a new Feast release.
