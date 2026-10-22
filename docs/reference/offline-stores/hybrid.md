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
  type: hybrid
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

## Using ConnectionRef with Hybrid Offline Store

When using the HybridOfflineStore, each data source can carry its own credentials via `ConnectionRef`. This is particularly useful when different feature views connect to different accounts or clusters — you no longer need to embed all credentials in `feature_store.yaml`.

### Example: Per-DataSource Credentials

{% code title="feature_store.yaml" %}
```yaml
project: my_feature_repo
registry: data/registry.db
provider: local
offline_store:
  type: hybrid_offline_store.HybridOfflineStore
  offline_stores:
    - type: snowflake.offline
    - type: bigquery
```
{% endcode %}

```python
from feast import FeatureView, Entity, ValueType
from feast.credentials import ConnectionRef
from feast.infra.offline_stores.snowflake_source import SnowflakeSource
from feast.infra.offline_stores.bigquery_source import BigQuerySource

entity = Entity(name="user_id", value_type=ValueType.INT64, join_keys=["user_id"])

# Snowflake source with credentials from a Kubernetes Secret
feature_view1 = FeatureView(
    name="user_features",
    entities=["user_id"],
    ttl=None,
    source=SnowflakeSource(
        table="USER_FEATURES",
        connection_ref=ConnectionRef(
            provider="kubernetes",
            name="snowflake-team-a-creds",
            namespace="ml-team",
            connection_type="snowflake.offline",
            params={"account": "xy12345", "warehouse": "COMPUTE_WH"},
        ),
    ),
)

# BigQuery source with credentials from a Kubernetes Secret
feature_view2 = FeatureView(
    name="user_activity",
    entities=["user_id"],
    ttl=None,
    source=BigQuerySource(
        table="my_project.dataset.user_activity",
        connection_ref=ConnectionRef(
            provider="kubernetes",
            name="bigquery-team-b-creds",
            namespace="ml-team",
            connection_type="bigquery",
        ),
    ),
)
```

In this setup:
- No sensitive credentials are stored in `feature_store.yaml`.
- Each data source resolves its credentials independently at runtime from the referenced Kubernetes Secret.
- The HybridOfflineStore routes operations to the correct backend based on the source type.

### How credential resolution works

1. The HybridOfflineStore determines which backend to use based on the data source class (e.g., `SnowflakeSource` → Snowflake offline store).
2. Before connecting, the offline store checks if the data source has a `connection_ref`.
3. If present, credentials are fetched from the external provider (e.g., reading a Kubernetes Secret).
4. Resolved credentials override the global offline store config for that operation.
5. If no `connection_ref` is set, the global `feature_store.yaml` configuration is used as a fallback.

This pattern is especially valuable in multi-tenant environments where a shared Feast deployment serves multiple teams, each with isolated credentials and backend accounts.

For details on the `ConnectionRef` structure and supported providers, see [Data Sources Overview](../data-sources/overview.md#per-datasource-credentials-connectionref).

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
