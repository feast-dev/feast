# Feature retrieval

## Overview

Generally, Feast supports several patterns of feature retrieval:

1. Training data generation (via `feature_store.get_historical_features(...)`)
2. Offline feature retrieval for batch scoring (via `feature_store.get_historical_features(...)`)
3. Online feature retrieval for real-time model predictions 
   - via the SDK: `feature_store.get_online_features(...)`
   - via deployed feature server endpoints: `requests.post('http://localhost:6566/get-online-features', data=json.dumps(online_request))` 

Each of these retrieval mechanisms accept:

* some way of specifying entities (to fetch features for)
* some way to specify the features to fetch (either via [feature services](feature-retrieval.md#feature-services), which group features needed for a model version, or [feature references](feature-retrieval.md#feature-references))

Before beginning, you need to instantiate a local `FeatureStore` object that knows how to parse the registry (see [more details](https://docs.feast.dev/getting-started/concepts/registry))

For code examples of how the below work, inspect the generated repository from `feast init -t [YOUR TEMPLATE]` (`gcp`, `snowflake`, and `aws` are the most fully fleshed).

## Concepts
Before diving into how to retrieve features, we need to understand some high level concepts in Feast.

### Feature Services

A feature service is an object that represents a logical group of features from one or more [feature views](feature-view.md#feature-view). Feature Services allows features from within a feature view to be used as needed by an ML model. Users can expect to create one feature service per model version, allowing for tracking of the features used by models.

{% tabs %}
{% tab title="driver_trips_feature_service.py" %}
```python
from driver_ratings_feature_view import driver_ratings_fv
from driver_trips_feature_view import driver_stats_fv

driver_stats_fs = FeatureService(
    name="driver_activity",
    features=[driver_stats_fv, driver_ratings_fv[["lifetime_rating"]]]
)
```
{% endtab %}
{% endtabs %}

Feature services are used during

* The generation of training datasets when querying feature views in order to find historical feature values. A single training dataset may consist of features from multiple feature views.
* Retrieval of features for batch scoring from the offline store (e.g. with an entity dataframe where all timestamps are `now()`)
* Retrieval of features from the online store for online inference (with smaller batch sizes). The features retrieved from the online store may also belong to multiple feature views.

{% hint style="info" %}
Applying a feature service does not result in an actual service being deployed.
{% endhint %}

Feature services enable referencing all or some features from a feature view.

Retrieving from the online store with a feature service

```python
from feast import FeatureStore
feature_store = FeatureStore('.')  # Initialize the feature store

feature_service = feature_store.get_feature_service("driver_activity")
features = feature_store.get_online_features(
    features=feature_service, entity_rows=[entity_dict]
)
```

Retrieving from the offline store with a feature service

```python
from feast import FeatureStore
feature_store = FeatureStore('.')  # Initialize the feature store

feature_service = feature_store.get_feature_service("driver_activity")
feature_store.get_historical_features(features=feature_service, entity_df=entity_df)
```

### Feature References

This mechanism of retrieving features is only recommended as you're experimenting. Once you want to launch experiments or serve models, feature services are recommended.

Feature references uniquely identify feature values in Feast. The structure of a feature reference in string form is as follows: `<feature_view>:<feature>`

Feature references are used for the retrieval of features from Feast:

```python
online_features = fs.get_online_features(
    features=[
        'driver_locations:lon',
        'drivers_activity:trips_today'
    ],
    entity_rows=[
        # {join_key: entity_value}
        {'driver': 'driver_1001'}
    ]
)
```

It is possible to retrieve features from multiple feature views with a single request, and Feast is able to join features from multiple tables in order to build a training dataset. However, it is not possible to reference (or retrieve) features from multiple projects at the same time.

{% hint style="info" %}
Note, if you're using [Feature views without entities](feature-view.md#feature-views-without-entities), then those features can be added here without additional entity values in the `entity_rows` parameter.
{% endhint %}

### Event timestamp

The timestamp on which an event occurred, as found in a feature view's data source. The event timestamp describes the event time at which a feature was observed or generated.

Event timestamps are used during point-in-time joins to ensure that the latest feature values are joined from feature views onto entity rows. Event timestamps are also used to ensure that old feature values aren't served to models during online serving.

### Dataset

A dataset is a collection of rows that is produced by a historical retrieval from Feast in order to train a model. A dataset is produced by a join from one or more feature views onto an entity dataframe. Therefore, a dataset may consist of features from multiple feature views.

**Dataset vs Feature View:** Feature views contain the schema of data and a reference to where data can be found (through its data source). Datasets are the actual data manifestation of querying those data sources.

**Dataset vs Data Source:** Datasets are the output of historical retrieval, whereas data sources are the inputs. One or more data sources can be used in the creation of a dataset.

## Retrieving historical features (for training data or batch scoring)
Feast abstracts away point-in-time join complexities with the `get_historical_features` API.

We go through the major steps, and also show example code. Note that the quickstart templates generally have end-to-end working examples for all these cases.

<details>

<summary>Full example: generate training data</summary>

```python
entity_df = pd.DataFrame.from_dict(
    {
        "driver_id": [1001, 1002, 1003, 1004, 1001],
        "event_timestamp": [
            datetime(2021, 4, 12, 10, 59, 42),
            datetime(2021, 4, 12, 8, 12, 10),
            datetime(2021, 4, 12, 16, 40, 26),
            datetime(2021, 4, 12, 15, 1, 12),
            datetime.now()
        ]
    }
)
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=store.get_feature_service("model_v1"),
).to_df()
print(training_df.head())
```

</details>

<details>

<summary>Full example: retrieve offline features for batch scoring</summary>

The main difference here compared to training data generation is how to handle timestamps in the entity dataframe. You want to pass in the **current time** to get the latest feature values for all your entities.

```python
from feast import FeatureStore

store = FeatureStore(repo_path=".")

# Get the latest feature values for unique entities
entity_sql = f"""
    SELECT
        driver_id,
        CURRENT_TIMESTAMP() as event_timestamp
    FROM {store.get_data_source("driver_hourly_stats_source").get_table_query_string()}
    WHERE event_timestamp BETWEEN '2021-01-01' and '2021-12-31'
    GROUP BY driver_id
"""
batch_scoring_features = store.get_historical_features(
    entity_df=entity_sql,
    features=store.get_feature_service("model_v2"),
).to_df()
# predictions = model.predict(batch_scoring_features)
```

</details>

### Step 1: Specifying Features
Feast accepts either:
- [feature services](feature-retrieval.md#feature-services), which group features needed for a model version
- [feature references](feature-retrieval.md#feature-references)

#### Example: querying a feature service (recommended)
```python
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=store.get_feature_service("model_v1"),
).to_df()
```

#### Example: querying a list of feature references
```python
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_daily_features:daily_miles_driven"
    ],
).to_df()
```
### Step 2: Specifying Entities
Feast accepts either a **Pandas dataframe** as the entity dataframe (including entity keys and timestamps) or a **SQL query** to generate the entities.

Both approaches must specify the full **entity key** needed as well as the **timestamps**. Feast then joins features onto this dataframe.

#### Example: entity dataframe for generating training data
```python
entity_df = pd.DataFrame.from_dict(
    {
        "driver_id": [1001, 1002, 1003, 1004, 1001],
        "event_timestamp": [
            datetime(2021, 4, 12, 10, 59, 42),
            datetime(2021, 4, 12, 8, 12, 10),
            datetime(2021, 4, 12, 16, 40, 26),
            datetime(2021, 4, 12, 15, 1, 12),
            datetime.now()
        ]
    }
)
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_daily_features:daily_miles_driven"
    ],
).to_df()
```

#### Example: entity SQL query for generating training data
You can also pass a SQL string to generate the above dataframe. This is useful for getting all entities in a timeframe from some data source.

```python
entity_sql = f"""
    SELECT
        driver_id,
        event_timestamp
    FROM {store.get_data_source("driver_hourly_stats_source").get_table_query_string()}
    WHERE event_timestamp BETWEEN '2021-01-01' and '2021-12-31'
"""
training_df = store.get_historical_features(
    entity_df=entity_sql,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_daily_features:daily_miles_driven"
    ],
).to_df()
```

## Retrieving online features (for model inference)
Feast will ensure the latest feature values for registered features are available. At retrieval time, you need to supply a list of **entities** and the corresponding **features** to be retrieved. Similar to `get_historical_features`, we recommend using feature services as a mechanism for grouping features in a model version.

_Note: unlike `get_historical_features`, the `entity_rows`  **do not need timestamps** since you only want one feature value per entity key._

There are several options for retrieving online features: through the SDK, or through a feature server

<details>

<summary>Full example: retrieve online features for real-time model inference (Python SDK)</summary>

```python
from feast import RepoConfig, FeatureStore
from feast.repo_config import RegistryConfig

repo_config = RepoConfig(
    registry=RegistryConfig(path="gs://feast-test-gcs-bucket/registry.pb"),
    project="feast_demo_gcp",
    provider="gcp",
)
store = FeatureStore(config=repo_config)

features = store.get_online_features(
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_daily_features:daily_miles_driven",
    ],
    entity_rows=[
        {
            "driver_id": 1001,
        }
    ],
).to_dict()
```

</details>

<details>

<summary>Full example: retrieve online features for real-time model inference (Feature Server)</summary>

This approach requires you to deploy a feature server (see [Python feature server](../../reference/feature-servers/python-feature-server)).

```python
import requests
import json

online_request = {
    "features": [
        "driver_hourly_stats:conv_rate",
    ],
    "entities": {"driver_id": [1001, 1002]},
}
r = requests.post('http://localhost:6566/get-online-features', data=json.dumps(online_request))
print(json.dumps(r.json(), indent=4, sort_keys=True))
```

</details>
