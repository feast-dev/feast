# Hybrid Offline Store

## Description
The HybridOfflineStore allows routing offline feature operations to different offline store backends based on the `batch_source` of the FeatureView. This enables a single Feast deployment to support multiple offline store backends, each configured independently and selected dynamically at runtime.

## Getting started
To use the HybridOfflineStore, install Feast with all required offline store dependencies (e.g., BigQuery, Snowflake, etc.) for the stores you plan to use. For example:

```bash
pip install 'feast[spark,snowflake]'
```

## Example

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
offline_store:
  type: hybrid_offline_store.HybridOfflineStore
  offline_stores:
    - type: spark
      conf:
        spark_master: local[*]
        spark_app_name: feast_spark_app
    - type: snowflake
      conf:
        account: my_snowflake_account
        user: feast_user
        password: feast_password
        database: feast_database
        schema: feast_schema
```
{% endcode %}

### Example FeatureView 
```python
from feast import FeatureView, Entity, ValueType
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.infra.offline_stores.snowflake_source import SnowflakeSource


entity = Entity(name="user_id", value_type=ValueType.INT64, join_keys=["user_id"])
feature_view1 = FeatureView(
    name="user_features",
    entities=["user_id"],
    ttl=None,
    features=[
        # Define your features here
    ],
    source=SparkSource(
        path="s3://my-bucket/user_features_data",
    ),
)

feature_view2 = FeatureView(
    name="user_activity",
    entities=["user_id"],
    ttl=None,
    features=[
        # Define your features here
    ],
    source=SnowflakeSource(
        path="s3://my-bucket/user_activity_data",
    ),
)

```

Then you can use materialize API to materialize the data from the specified offline store based on the `batch_source` of the FeatureView.

```python
from feast import FeatureStore
store = FeatureStore(repo_path=".")
store.materialize(
    start_date="2025-01-01",
    end_date="2025-07-31",
    feature_views=[feature_view1, feature_view2],
)
```

## Functionality Matrix
| Feature/Functionality                             | Supported                  |
|---------------------------------------------------|----------------------------|
| pull_latest_from_table_or_query                   | Yes                        |
| pull_all_from_table_or_query                      | Yes                        |
| offline_write_batch                               | Yes                        |
| validate_data_source                              | Yes                        |
| get_table_column_names_and_types_from_data_source | Yes                        |
| write_logged_features                             | No                         |
| get_historical_features                           | Only with same data source |
