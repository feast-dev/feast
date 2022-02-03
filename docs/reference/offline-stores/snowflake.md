# Snowflake

## Description

The Snowflake offline store provides support for reading [SnowflakeSources](../data-sources/snowflake.md).

* Snowflake tables and views are allowed as sources.
* All joins happen within Snowflake.
* Entity dataframes can be provided as a SQL query or can be provided as a Pandas dataframe. Pandas dataframes will be uploaded to Snowflake in order to complete join operations.
* A `SnowflakeRetrievalJob` is returned when calling `get_historical_features()`.
  * This allows you to call
     * `to_snowflake` to save the dataset into Snowflake
     * `to_sql` to get the SQL query that would execute on `to_df`
     * `to_arrow_chunks` to get the result in batches ([Snowflake python connector docs](https://docs.snowflake.com/en/user-guide/python-connector-api.html#get_result_batches)) 

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

Configuration options are available in [SnowflakeOfflineStoreConfig](https://github.com/feast-dev/feast/blob/master/sdk/python/feast/infra/offline_stores/snowflake.py#L56).
