# Feature retrieval

## Dataset

A dataset is a collection of rows that is produced by a historical retrieval from Feast in order to train a model. A dataset is produced by a join from one or more feature views onto an entity dataframe. Therefore, a dataset may consist of features from multiple feature views.

**Dataset vs Feature View:** Feature views contain the schema of data and a reference to where data can be found \(through its data source\). Datasets are the actual data manifestation of querying those data sources.

**Dataset vs Data Source:** Datasets are the output of historical retrieval, whereas data sources are the inputs. One or more data sources can be used in the creation of a dataset.

## Feature Services
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

## Feature References

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

It is possible to retrieve features from multiple feature views with a single request, and Feast is able to join features from multiple tables in order to build a training dataset. However, It is not possible to reference \(or retrieve\) features from multiple projects at the same time.

{% hint style="info" %}
Note, if you're using [Feature views without entities](feature-view.md#feature-views-without-entities), then those features can be added here without additional entity values in the `entity_rows`
{% endhint %}

## Event timestamp

The timestamp on which an event occurred, as found in a feature view's data source. The event timestamp describes the event time at which a feature was observed or generated.

Event timestamps are used during point-in-time joins to ensure that the latest feature values are joined from feature views onto entity rows. Event timestamps are also used to ensure that old feature values aren't served to models during online serving.

