# Feature retrieval

## 1. Overview

Feature retrieval \(or serving\) is the process of retrieving either historical features or online features from Feast, for the purposes of training or serving a model.

Feast attempts to unify the process of retrieving features in both the historical and online case. It does this through the creation of feature references. One of the major advantages of using Feast is that you have a single semantic reference to a feature. These feature references can then be stored alongside your model and loaded into a serving layer where it can be used for online feature retrieval.

## 2. Feature references

In Feast, each feature can be uniquely addressed through a feature reference. A feature reference is composed of the following components

* Feature Set
* Feature

These components can be used to create a string based feature reference as follows

`<feature-set>:<feature>`

Feast will attempt to infer both the `feature-set` name if it is not provided, but a feature reference must provide a `feature` name.

```python
# Feature references
features = [
 'partner', 
 'daily_transactions',
 'customer_feature_set:dependents',
 'customer_feature_set:has_phone_service',
 ]

target = 'churn'
```

{% hint style="info" %}
Where the Features from different Feature Sets use the same name, the Feature Set name `feature-set` is required to disambiguate which feature is specified.
{% endhint %}

Feature references only apply to a single `project`. Features cannot be retrieved across projects in a single request.

## 3. Historical feature retrieval

Historical feature retrieval can be done through either the Feast SDK or directly through the Feast Serving gRPC API. Below is an example of historical retrieval from the [Churn Prediction Notebook](https://github.com/gojek/feast/blob/master/examples/feast-xgboost-churn-prediction-tutorial/Telecom%20Customer%20Churn%20Prediction%20%28with%20Feast%20and%20XGBoost%29.ipynb).

```python
# Add the target variable to our feature list
features = self._features + [self._target]

# Retrieve training dataset from Feast. The "entity_df" is a dataframe that contains
# timestamps and entity keys. In this case, it is a dataframe with two columns.
# One timestamp column, and one customer id column
dataset = client.get_historical_features(
    feature_refs=features,
    entity_rows=entity_df
)

# Materialize the dataset object to a Pandas DataFrame. 
# Alternatively it is possible to use a file reference if the data is too large
df = dataset.to_dataframe()
```

{% hint style="info" %}
When no project is specified when retrieving features with `get_historical_features()`, Feast infers that the features specified belong to the `default` project. To retrieve from another project, specify the `default` parameter when retrieving features.
{% endhint %}

In the above example, Feast does a point in time correct query from a single feature set. For each timestamp and entity key combination that is provided by `entity_df`, Feast determines the values of all the features in the `features` list at that respective point in time and then joins features values to that specific entity value and timestamp, and repeats this process for all timestamps.

This is called a point in time correct join.

Feast allows users to retrieve features from any feature sets and join them together in a single response dataset. The only requirement is that the user provides the correct entities in order to look up the features.

### **Point-in-time-correct Join**

Below is another example of how a point-in-time-correct join works. We have two dataframes. The first is the `entity dataframe` that contains timestamps, entities, and labels. The user would like to have driver features joined onto this `entity dataframe` from the `driver dataframe` to produce an `output dataframe` that contains both labels and features. They would then like to train their model on this output

![Input 1: Entity DataFrame](https://lh3.googleusercontent.com/ecS5sqj3FHLFSm06XF11NmTQSru-bQ4Az3Kuko_vg5YlBxXjHadlsGwmo7d7wUx4fA1ssdZvxrESDKfkGWjj3HNJg_jIqXY0avz2JzCcEOXLBLmtXNEY8k2u3f4QusHdDWdqRARQHYE)

![Input 2: Driver DataFrame](https://lh3.googleusercontent.com/LRtCOzmcfhLWzpyndbRKZSVPanLLzfULoHx2YxY6N3i1gQd2Eh6MS1igahOe8ydA7zQulIFJEaQ0IXFXOsdkKRobOC6ThSOnT4hACbCl1jeM4O2JDVC_kvw8lwTCezVUD3d6ZUYj31Q)

Typically the `input 1` DataFrame would be provided by the user, and the `input 2` DataFrame would already be ingested into Feast. To join these two, the user would call Feast as follows:

```python
# Feature references
features = [
 'conv_rate',
 'acc_rate',
 'avg_daily_trips',
 'trip_completed'
 ]


dataset = client.get_historical_features(
        feature_refs=features, # this is a list of feature references
        entity_rows=entity_df # This is the entity dataframe above
    )

# This prints out the dataframe below 
print(dataset.to_dataframe())
```

![Output: Joined DataFrame](https://lh5.googleusercontent.com/Gm-4Ru68KyIQ2tQtaVTDFngqO7pMtlMP1YAQO-bqln6_Mo2XAPdbij6w5ACnHAmQ053XUPu6G-c2aYRVJxPqPTMN_BcH6PY0-E1kCwXQAdW1CcQo5tc0g5ilcuVAtqsHcJB1R5mBdLo)

Feast is able to intelligently join feature data with different timestamps to a single basis table in a point-in-time-correct way. This allows users to join daily batch data with high-frequency event data transparently. They simply need to know the feature names.

{% hint style="info" %}
Feast can retrieve features from any amount of feature sets, as long as they occur on the same entities.
{% endhint %}

Point-in-time-correct joins also prevents the occurrence of feature leakage by trying to accurate the state of the world at a single point in time, instead of just joining features based on the nearest timestamps.

### **Computing statistics over retrieved data**

Feast is able to compute [TFDV](https://tensorflow.google.cn/tfx/tutorials/data_validation/tfdv_basic) compatible statistics over data retrieved from historical stores. The statistics can be used in conjunction with feature schemas and TFDV to verify the integrity of your retrieved dataset, or to [Facets](https://github.com/PAIR-code/facets) to visualize the distribution.

The computation of statistics is not enabled by default. To indicate to Feast that the statistics are to be computed for a given historical retrieval request, pass `compute_statistics=True` to `get_historical_features`.

```python
dataset = client.get_historical_features(
    feature_refs=features, 
    entity_rows=entity_df 
    compute_statistics=True
)

stats = dataset.statistics()
```

If a schema is already defined over the feature sets on question, tfdv can be used to detect anomalies over the dataset.

```python
# Build combined schema over retrieved dataset
schema = schema_pb2.Schema()
for feature_set in feature_sets:
    fs_schema = feature_set.export_tfx_schema()
    for feature_schema in fs_schema.feature:
        if feature_schema.name in features:
            schema.feature.append(feature_schema)

# detect anomalies
anomalies = tfdv.validate_statistics(statistics=stats, schema=schema)
```

## Online feature retrieval

Online feature retrieval works in much the same way as batch retrieval, with one important distinction: Online stores only maintain the current state of features. No historical data is served.

```python
features = [
 'conv_rate',
 'acc_rate',
 'avg_daily_trips',
 ]

data = client.get_online_features(
        feature_refs=features, # Contains only feature references
        entity_rows=entity_rows, # Contains only entities (driver ids)
    )
```

{% hint style="info" %}
When no project is specified when retrieving features with get\_online\_feature\(\), Feast infers that the features specified belong to the default project. To retrieve from another project, specify the project parameter when retrieving features.
{% endhint %}

Online serving with Feast is built to be very low latency. Feast Serving provides a [gRPC API](https://api.docs.feast.dev/grpc/feast.serving.pb.html) that is backed by [Redis](https://redis.io/). We also provide support for [Python](https://api.docs.feast.dev/python/), [Go](https://godoc.org/github.com/gojek/feast/sdk/go), and Java clients.

