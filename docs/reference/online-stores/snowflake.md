# Snowflake online store

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

## Getting started
In order to use this online store, you'll need to run `pip install 'feast[snowflake]'`. You can then get started with the command `feast init REPO_NAME -t snowflake`.

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

## Tags KWARGs Actions:

"snowflake-online-store/online_path": Adding the "snowflake-online-store/online_path" key to a FeatureView tags parameter allows you to choose the online table path for the online serving table (ex. "{database}"."{schema}").

{% code title="example_config.py" %}
```python
driver_stats_fv = FeatureView(
    ...
    tags={"snowflake-online-store/online_path": '"FEAST"."ONLINE"'},
)
```
{% endcode %}

The full set of configuration options is available in [SnowflakeOnlineStoreConfig](https://rtd.feast.dev/en/latest/#feast.infra.online_stores.snowflake.SnowflakeOnlineStoreConfig).

## Functionality Matrix

The set of functionality supported by online stores is described in detail [here](overview.md#functionality).
Below is a matrix indicating which functionality is supported by the Snowflake online store.

|                                                           | Snowflake |
| :-------------------------------------------------------- | :-------- |
| write feature values to the online store                  | yes       |
| read feature values from the online store                 | yes       |
| update infrastructure (e.g. tables) in the online store   | yes       |
| teardown infrastructure (e.g. tables) in the online store | yes       |
| generate a plan of infrastructure changes                 | no        |
| support for on-demand transforms                          | yes       |
| readable by Python SDK                                    | yes       |
| readable by Java                                          | no        |
| readable by Go                                            | no        |
| support for entityless feature views                      | yes       |
| support for concurrent writing to the same key            | no        |
| support for ttl (time to live) at retrieval               | no        |
| support for deleting expired data                         | no        |
| collocated by feature view                                | yes       |
| collocated by feature service                             | no        |
| collocated by entity key                                  | no        |

To compare this set of functionality against other online stores, please see the full [functionality matrix](overview.md#functionality-matrix).
