# Snowflake

## Description

The Snowflake offline store provides support for reading [SnowflakeSources](../data-sources/snowflake.md).

* Snowflake tables and views are allowed as sources.
* All joins happen within Snowflake.
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. Pandas dataframes will be uploaded to Snowflake in order to complete join operations.
* A [SnowflakeRetrievalJob](https://github.com/feast-dev/feast/blob/bf557bcb72c7878a16dccb48443bbbe9dc3efa49/sdk/python/feast/infra/offline_stores/snowflake.py#L185) is returned when calling `get_historical_features()`.

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
offline_store:
  type: snowflake.offline
  account: snowflake_deployment.us-east-1
  user: user_login
  password: user_password
  role: sysadmin
  warehouse: demo_wh
  database: FEAST
```
{% endcode %}

Configuration options are available [here](https://github.com/feast-dev/feast/blob/bf557bcb72c7878a16dccb48443bbbe9dc3efa49/sdk/python/feast/infra/offline_stores/snowflake.py#L39).
