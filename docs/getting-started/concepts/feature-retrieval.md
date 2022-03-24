# Feature retrieval

## Dataset

A dataset is a collection of rows that is produced by a historical retrieval from Feast in order to train a model. A dataset is produced by a join from one or more feature views onto an entity dataframe. Therefore, a dataset may consist of features from multiple feature views.

**Dataset vs Feature View:** Feature views contain the schema of data and a reference to where data can be found \(through its data source\). Datasets are the actual data manifestation of querying those data sources.

**Dataset vs Data Source:** Datasets are the output of historical retrieval, whereas data sources are the inputs. One or more data sources can be used in the creation of a dataset.

## Feature References

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

